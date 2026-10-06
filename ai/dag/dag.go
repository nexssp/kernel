package dag

import (
	"context"
	"errors"
	"fmt"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
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
	Key   string
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
//
// A DAG is either static (only AddEdge calls, executed layer by layer)
// or dynamic (at least one AddConditionalEdge, executed node by node
// starting from entryID and following RouterFunc decisions). The two
// modes are mutually exclusive; see Execute.
type DAG struct {
	name             string
	nodes            map[string]Node
	edges            []Edge
	conditionalEdges map[string]conditionalEdge
	layers           [][]Node
	maxSteps         int
	entryID          string
}

// Name returns the DAG's identifier.
func (d *DAG) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// IsDynamic reports whether the DAG uses conditional routing.
func (d *DAG) IsDynamic() bool {
	if d == nil {
		return false
	}
	return len(d.conditionalEdges) > 0
}

// AsAction wraps the DAG as a standard Nexss action. The wrapped action
// accepts a ReadState and returns a freshly owned *State that the caller
// must Release.
func (d *DAG) AsAction() *action.Builder[ReadState, *State] {
	return action.New(d.name, func(ctx context.Context, state ReadState) (*State, error) {
		return d.Execute(ctx, state)
	}).Tag("graph", "orchestration")
}

// executeDynamic walks the graph node by node, following conditional
// edges until a router returns "" or "end", or until maxSteps is
// exceeded.
//
// The starting node is d.entryID. Every visited node's output is merged
// into the running state, so routers see the cumulative result of the
// path so far.
//
// The step limit is a hard bound: a router that keeps returning the
// same target terminates with xerr.Timeout rather than looping forever.
// The default limit is 32 when maxSteps is 0 or negative.
//
// LayerCallback is not fired: dynamic execution has no layers to
// checkpoint. ErrSuspended propagates intact, but no state is persisted
// on the callback's behalf; use action.Durable per node instead.
func (d *DAG) executeDynamic(ctx context.Context, initialState ReadState) (*State, error) {
	currentState := initialState.Clone()

	if d.entryID == "" {
		return currentState, xerr.Internal(fmt.Sprintf(
			"graph %q: dynamic execution without an entry node", d.name))
	}

	currentNodeID := d.entryID
	limit := d.maxSteps
	if limit <= 0 {
		limit = 32
	}

	// Label of the edge that routed to currentNodeID. Empty until the
	// first hop; included in the "node not found" diagnostic so a
	// mis-configured router names itself.
	routedFrom := ""

	for range limit {
		if err := ctx.Err(); err != nil {
			return currentState, err
		}

		node, exists := d.nodes[currentNodeID]
		if !exists {
			return currentState, missingNodeError(d.name, currentNodeID, routedFrom)
		}

		var output Value
		runErr := executeNode(ctx, node, currentState, &output)

		// Merge any node output before returning on error: a failed
		// nested DAG stashes its inner *State under InnerStateKey so a
		// resume caller sees the same partial state that layer mode
		// delivers via handleLayerError.
		if output.Key != "" && output.Val != nil {
			currentState.Set(output.Key, output.Val)
		}

		if runErr != nil {
			return currentState, attachDynamicState(runErr, currentState)
		}

		edge, isConditional := d.conditionalEdges[currentNodeID]
		if !isConditional {
			return currentState, nil
		}

		nextTarget, err := edge.Router(ctx, currentState.AsRead())
		if err != nil {
			return currentState, err
		}
		if nextTarget == "" || nextTarget == "end" {
			return currentState, nil
		}
		routedFrom = edge.Label
		currentNodeID = nextTarget
	}

	return currentState, xerr.Timeout(fmt.Sprintf(
		"graph %q: dynamic execution exceeded %d steps (router cycle?)",
		d.name, limit))
}

// missingNodeError builds the diagnostic for a router that pointed at a
// node that does not exist. When routedFrom is set, the message names
// the edge so the mis-configured router is identifiable.
func missingNodeError(graphName, nodeID, routedFrom string) error {
	if routedFrom == "" {
		return xerr.NotFound(fmt.Sprintf(
			"graph %q: node %q not found during dynamic execution",
			graphName, nodeID))
	}
	return xerr.NotFound(fmt.Sprintf(
		"graph %q: edge %q routed to unknown node %q",
		graphName, routedFrom, nodeID))
}

// attachDynamicState binds the running *State to an *ExecutionError from
// a node so a resume caller can persist it. ErrSuspended and any other
// error pass through unchanged.
func attachDynamicState(err error, state *State) error {
	if execErr, ok := errors.AsType[*ExecutionError](err); ok {
		execErr.State = state
		return execErr
	}
	return err
}
