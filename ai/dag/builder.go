package dag

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

// Builder accumulates nodes and edges, then compiles them into an
// immutable DAG. The first builder error short-circuits further calls.
type Builder struct {
	name             string
	nodes            map[string]Node
	edges            []Edge
	conditionalEdges map[string]conditionalEdge
	maxSteps         int
	entryID          string
	compileErr       error
}

// New returns a Builder for a named DAG.
func New(name string) *Builder {
	return &Builder{name: name, nodes: make(map[string]Node)}
}

// AddNode registers a node. id must be unique and non-empty; act must
// implement action.Executable. The second argument (a display name) is
// accepted for API compatibility and ignored — the node's output key is
// always derived from id.
func (b *Builder) AddNode(id, _ string, act action.AnyAction) *Builder {
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
	b.nodes[id] = Node{ID: id, OutputKey: OutputKey(id), Action: ex}
	if b.entryID == "" {
		b.entryID = id
	}
	return b
}

// AddEdge declares that `to` depends on `from`. Duplicate edges are
// rejected at build time.
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

// Compile validates the graph and produces the immutable DAG. Errors
// accumulated by AddNode / AddEdge / AddConditionalEdge surface here.
func (b *Builder) Compile() (*DAG, error) {
	if b.compileErr != nil {
		return nil, b.compileErr
	}

	// Validate conditional edges point at existing nodes BEFORE we
	// build the layer graph. A router that returns an unknown node ID
	// at runtime becomes xerr.NotFound; a router whose source node is
	// unknown is a compile error.
	for from := range b.conditionalEdges {
		if _, ok := b.nodes[from]; !ok {
			return nil, xerr.NotFound(fmt.Sprintf(
				"graph compile: conditional edge source %q not found", from))
		}
	}

	// Dynamic DAGs must have a resolvable entry node.
	if len(b.conditionalEdges) > 0 {
		if b.entryID == "" {
			return nil, xerr.BadRequest(fmt.Sprintf(
				"graph compile: dynamic graph %q has no entry node", b.name))
		}
		if _, ok := b.nodes[b.entryID]; !ok {
			return nil, xerr.NotFound(fmt.Sprintf(
				"graph compile: entry node %q not found", b.entryID))
		}
	}

	layers, visited, err := b.computeLayers()
	if err != nil {
		return nil, err
	}

	// A graph with only conditional edges has no static edges and
	// therefore no layers. That is valid — the entry point is the
	// single node with no incoming static edges. Detect the degenerate
	// case explicitly: dynamic DAGs need at least one node to start
	// from.
	if len(b.conditionalEdges) > 0 && len(b.nodes) == 0 {
		return nil, xerr.BadRequest("graph compile: dynamic graph has no nodes")
	}

	if len(b.conditionalEdges) == 0 && visited != len(b.nodes) {
		return nil, xerr.Conflict(fmt.Sprintf(
			"graph compile: cycle detected in graph %q", b.name))
	}

	// Copy conditional edges so the DAG does not alias the builder's
	// map if the builder is reused.
	var condEdges map[string]conditionalEdge
	if len(b.conditionalEdges) > 0 {
		condEdges = make(map[string]conditionalEdge, len(b.conditionalEdges))
		maps.Copy(condEdges, b.conditionalEdges)
	}

	return &DAG{
		name:             b.name,
		nodes:            b.nodes,
		edges:            slices.Clone(b.edges),
		conditionalEdges: condEdges,
		layers:           layers,
		maxSteps:         b.maxSteps,
		entryID:          b.entryID,
	}, nil
}

func (b *Builder) computeLayers() ([][]Node, int, error) {
	inDegree := make(map[string]int, len(b.nodes))
	adj := make(map[string][]string, len(b.nodes))

	for id := range b.nodes {
		inDegree[id] = 0
	}
	for _, e := range b.edges {
		if _, ok := b.nodes[e.From]; !ok {
			return nil, 0, xerr.NotFound(fmt.Sprintf("graph compile: node %q not found", e.From))
		}
		if _, ok := b.nodes[e.To]; !ok {
			return nil, 0, xerr.NotFound(fmt.Sprintf("graph compile: node %q not found", e.To))
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
	return layers, visited, nil
}
