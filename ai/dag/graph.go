package dag

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"

	"golang.org/x/sync/errgroup"
)

// Value holds a key-value output produced by a node.
type Value struct {
	Key string
	Val any
}

// OutputKey returns the canonical collision-resistant state key for a node.
func OutputKey(nodeID string) string { return "tasks." + nodeID + ".output" }

// GetNodeOutput reads a node result using the allocation-free direct type path.
// A missing or mismatched value returns a structured error; durable replay
// adapters should decode serialized values before calling this helper.
func GetNodeOutput[T any](state *State, nodeID string) (T, error) {
	var zero T
	if state == nil {
		return zero, xerr.Internal("graph: cannot read output from nil state")
	}
	value, ok := state.Get(OutputKey(nodeID))
	if !ok {
		return zero, xerr.NotFound(fmt.Sprintf("graph: node %q output not found", nodeID))
	}
	typed, ok := value.(T)
	if !ok {
		return zero, xerr.Validation(fmt.Sprintf("graph: node %q output has type %T, expected %T", nodeID, value, zero))
	}
	return typed, nil
}

// State is an immutable-by-convention view of graph state.
// Reads are 100% lock-free because data is never mutated while being read.
type State struct {
	data map[string]any
}

var statePool = sync.Pool{
	New: func() any {
		return &State{data: make(map[string]any, 16)}
	},
}

// AcquireState retrieves a State instance from the memory pool.
func AcquireState() *State {
	return statePool.Get().(*State)
}

// Release clears references and returns the State to the pool.
func (s *State) Release() {
	if s == nil || s.data == nil {
		return
	}
	clear(s.data)
	statePool.Put(s)
}

// Get performs a ZERO-LOCK read.
func (s *State) Get(key string) (any, bool) {
	if s == nil || s.data == nil {
		return nil, false
	}
	v, ok := s.data[key]
	return v, ok
}

// Set writes a key-value pair to the state.
func (s *State) Set(key string, val any) {
	if s == nil {
		return
	}
	if s.data == nil {
		s.data = make(map[string]any, 16)
	}
	s.data[key] = val
}

// Clone creates a new State snapshot.
func (s *State) Clone() *State {
	next := AcquireState()
	if s != nil && s.data != nil {
		maps.Copy(next.data, s.data)
	}
	return next
}

// CopyFrom populates s with src data, guaranteeing independent map headers.
func (s *State) CopyFrom(src *State) {
	if s == nil || src == nil {
		return
	}
	if s.data == nil {
		s.data = make(map[string]any, len(src.data))
	} else {
		clear(s.data)
	}
	if src.data != nil {
		maps.Copy(s.data, src.data)
	}
}

// Data exposes the internal map for serialization / rendering.
func (s *State) Data() map[string]any {
	if s == nil || s.data == nil {
		return nil
	}
	out := make(map[string]any, len(s.data))
	maps.Copy(out, s.data)
	return out
}

// ── Node & Execution Types ───────────────────────────────────────────────────

type NodeContext struct {
	Input *State // Read-Only, Frozen, Zero-Lock
	Key   string // Designated output key for this node
}

type Node struct {
	ID        string
	OutputKey string
	Action    action.Executable
}

type Edge struct {
	From string
	To   string
}

type DAG struct {
	name   string
	nodes  map[string]Node
	edges  []Edge
	layers [][]Node
}

type Builder struct {
	name       string
	nodes      map[string]Node
	edges      []Edge
	compileErr error
}

func New(name string) *Builder {
	return &Builder{
		name:  name,
		nodes: make(map[string]Node),
	}
}

func (b *Builder) AddNode(id string, _ string, act action.AnyAction) *Builder {
	if b.compileErr != nil {
		return b
	}
	if id == "" {
		b.compileErr = xerr.BadRequest("graph: node ID cannot be empty")
		return b
	}
	if _, exists := b.nodes[id]; exists {
		b.compileErr = xerr.Conflict(fmt.Sprintf("graph: duplicate node %q", id))
		return b
	}
	ex, ok := act.(action.Executable)
	if !ok || ex == nil {
		b.compileErr = xerr.BadRequest(fmt.Sprintf("graph: action %q does not implement Executable", id))
		return b
	}
	// The second parameter is retained for source compatibility, but output
	// storage is always namespaced to prevent parallel fan-out collisions.
	b.nodes[id] = Node{ID: id, OutputKey: OutputKey(id), Action: ex}
	return b
}

func (b *Builder) AddEdge(from, to string) *Builder {
	if b.compileErr != nil {
		return b
	}
	if from == "" || to == "" {
		b.compileErr = xerr.BadRequest("graph: edge endpoints cannot be empty")
		return b
	}
	for _, edge := range b.edges {
		if edge.From == from && edge.To == to {
			b.compileErr = xerr.Conflict(fmt.Sprintf("graph: duplicate edge %q -> %q", from, to))
			return b
		}
	}
	b.edges = append(b.edges, Edge{From: from, To: to})
	return b
}

func (b *Builder) Compile() (*DAG, error) {
	if b.compileErr != nil {
		return nil, b.compileErr
	}
	inDegree := make(map[string]int, len(b.nodes))
	adj := make(map[string][]string, len(b.nodes))

	for id := range b.nodes {
		inDegree[id] = 0
	}
	for _, e := range b.edges {
		if _, ok := b.nodes[e.From]; !ok {
			return nil, xerr.NotFound(fmt.Sprintf("graph compile: node %q not found", e.From))
		}
		if _, ok := b.nodes[e.To]; !ok {
			return nil, xerr.NotFound(fmt.Sprintf("graph compile: node %q not found", e.To))
		}
		adj[e.From] = append(adj[e.From], e.To)
		inDegree[e.To]++
	}

	var layers [][]Node
	visited := 0

	for {
		var readyIDs []string
		for id, deg := range inDegree {
			if deg == 0 {
				readyIDs = append(readyIDs, id)
			}
		}
		sort.Strings(readyIDs)
		currentLayer := make([]Node, 0, len(readyIDs))
		for _, id := range readyIDs {
			currentLayer = append(currentLayer, b.nodes[id])
		}

		if len(currentLayer) == 0 {
			break
		}

		for _, node := range currentLayer {
			delete(inDegree, node.ID)
			visited++
			for _, child := range adj[node.ID] {
				inDegree[child]--
			}
		}

		layers = append(layers, currentLayer)
	}

	if visited != len(b.nodes) {
		return nil, xerr.Conflict(fmt.Sprintf("graph compile: cycle detected in graph %q", b.name))
	}

	return &DAG{
		name:   b.name,
		nodes:  b.nodes,
		edges:  slices.Clone(b.edges),
		layers: layers,
	}, nil
}

// Execute runs DAG layers sequentially, executing nodes in each layer concurrently.
func (d *DAG) Execute(ctx context.Context, initialState *State) (*State, error) {
	currentState := initialState.Clone()

	for layerIdx, layer := range d.layers {
		g, layerCtx := errgroup.WithContext(ctx)
		layerResults := make([]Value, len(layer))

		for i, node := range layer {
			idx := i
			n := node

			g.Go(func() error {
				nCtx := &NodeContext{
					Input: currentState,
					Key:   n.OutputKey,
				}

				decoder := func(v any) error {
					switch target := v.(type) {
					case *NodeContext:
						*target = *nCtx
						return nil
					case **NodeContext:
						*target = nCtx
						return nil
					}
					return nil
				}

				var outVal any
				var err error
				if nested, ok := n.Action.(*action.BuiltAction[*State, *State]); ok {
					// A compiled graph is itself a typed action. Passing the
					// current immutable snapshot enables direct graph nesting.
					outVal, err = nested.Do(layerCtx, currentState)
				} else {
					outVal, err = n.Action.ExecuteDecoded(layerCtx, decoder)
				}

				if err != nil {
					return xerr.Internal(fmt.Sprintf("graph execution failed at node %q (layer %d)", n.ID, layerIdx), err)
				}

				layerResults[idx] = Value{
					Key: n.OutputKey,
					Val: outVal,
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			currentState.Release()
			return nil, err
		}

		nextState := currentState.Clone()
		currentState.Release()

		for _, res := range layerResults {
			if res.Key != "" && res.Val != nil {
				nextState.data[res.Key] = res.Val
			}
		}

		currentState = nextState
	}

	return currentState, nil
}

// AsAction wraps the DAG execution as a standard Nexss Action.
func (d *DAG) AsAction() *action.Builder[*State, *State] {
	return action.New(d.name, func(ctx context.Context, state *State) (*State, error) {
		return d.Execute(ctx, state)
	}).Tag("graph", "orchestration")
}
