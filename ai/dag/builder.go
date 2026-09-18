package dag

import (
	"fmt"
	"slices"
	"sort"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

// Builder accumulates nodes and edges, then compiles them into an
// immutable DAG. The first builder error short-circuits further calls.
type Builder struct {
	name       string
	nodes      map[string]Node
	edges      []Edge
	compileErr error
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
// accumulated by AddNode / AddEdge surface here.
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
