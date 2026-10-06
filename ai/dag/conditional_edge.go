package dag

import (
	"context"
	"fmt"

	"github.com/nexssp/kernel/xerr"
)

// RouterFunc decides the next node to visit after its owning edge's
// source node. Returning "" or "end" terminates the dynamic walk. An
// error aborts the walk and is returned to the caller unchanged.
type RouterFunc func(ctx context.Context, state ReadState) (targetNodeID string, err error)

// conditionalEdge is the compiled form of one AddConditionalEdge call.
//
// From is stored alongside the map key so error messages and future
// trace output can name the routing source without a reverse lookup.
// Label defaults to From and is what diagnostics display; use
// AddNamedConditionalEdge when the source node ID is not descriptive
// (e.g. "node_5" vs "approval_gate").
type conditionalEdge struct {
	From   string
	Label  string
	Router RouterFunc
}

// AddConditionalEdge makes the graph dynamic: execution walks node by
// node from the entry point, following RouterFunc decisions instead of
// running topological layers.
//
// Only one router per source node is allowed. The label defaults to
// the source node ID; see AddNamedConditionalEdge for a distinct one.
func (b *Builder) AddConditionalEdge(from string, router RouterFunc) *Builder {
	return b.addConditionalEdge(from, from, router)
}

// AddNamedConditionalEdge is AddConditionalEdge with an explicit label
// that appears in diagnostics when the router points at an unknown
// node.
func (b *Builder) AddNamedConditionalEdge(from, label string, router RouterFunc) *Builder {
	return b.addConditionalEdge(from, label, router)
}

func (b *Builder) addConditionalEdge(from, label string, router RouterFunc) *Builder {
	if b.compileErr != nil {
		return b
	}
	if from == "" || router == nil {
		b.compileErr = xerr.BadRequest(
			"graph: conditional edge requires non-empty source and router")
		return b
	}
	if label == "" {
		label = from
	}
	if b.conditionalEdges == nil {
		b.conditionalEdges = make(map[string]conditionalEdge)
	}
	if _, exists := b.conditionalEdges[from]; exists {
		b.compileErr = xerr.Conflict(fmt.Sprintf(
			"graph: conditional edge for %q already declared", from))
		return b
	}
	b.conditionalEdges[from] = conditionalEdge{
		From:   from,
		Label:  label,
		Router: router,
	}
	return b
}
