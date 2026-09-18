package dag

import (
	"context"

	"github.com/nexssp/kernel/action"
)

// NodeContext is the execution context handed to every node.
//
// Input is read-only by construction: the compiler enforces that nodes
// cannot mutate the parent layer's state. There is no Set method on
// ReadState, and the engine never exposes a *State through this struct.
//
// A node may retain Input past its own return (e.g. spawn a goroutine
// that reads it later) — the underlying map is guaranteed alive as long
// as any ReadState view of it exists.
type NodeContext struct {
	Input ReadState
	Key   string // designated output key for this node
}

// Node is a single unit of DAG work.
type Node struct {
	ID        string
	OutputKey string
	Action    action.Executable
}

// Edge is a directed dependency between two nodes.
type Edge struct {
	From string
	To   string
}

// DAG is an immutable, compiled directed acyclic graph.
type DAG struct {
	name   string
	nodes  map[string]Node
	edges  []Edge
	layers [][]Node
}

// Name returns the DAG's identifier.
func (d *DAG) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// AsAction wraps the DAG as a standard Nexss action. The wrapped action
// accepts a ReadState (typically the parent layer's view) and returns a
// freshly owned *State that the caller must Release.
//
// Nesting: the inner DAG clones the parent's state internally, so the
// parent is never mutated by the inner graph.
func (d *DAG) AsAction() *action.Builder[ReadState, *State] {
	return action.New(d.name, func(ctx context.Context, state ReadState) (*State, error) {
		// Execute takes a ReadState and clones internally.
		return d.Execute(ctx, state)
	}).Tag("graph", "orchestration")
}
