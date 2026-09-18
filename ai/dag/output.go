package dag

import (
	"fmt"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

// Value holds a key-value output produced by a node.
type Value struct {
	Key string
	Val any
}

const innerStateSuffix = ".inner_state"

// InnerStateKey returns the state key that holds a nested DAG's
// partially-completed inner state. On nested failure the parent DAG
// stashes the inner *State under this key so a resume run can continue
// the inner graph from where it stopped.
func InnerStateKey(nodeID string) string {
	return OutputKey(nodeID) + innerStateSuffix
}

// OutputKey returns the canonical collision-resistant state key for a
// node's output.
func OutputKey(nodeID string) string { return "tasks." + nodeID + ".output" }

// GetNodeOutput reads a node's typed output from a read-only state view.
//
// Hot path: single type assertion, zero allocations. Slow path (checkpoint
// resume): falls back to action.Coerce for JSON-decoded map values.
func GetNodeOutput[T any](state ReadState, nodeID string) (T, error) {
	var zero T
	if state.data == nil {
		return zero, xerr.Internal("graph: cannot read output from nil state")
	}
	value, ok := state.Get(OutputKey(nodeID))
	if !ok {
		return zero, xerr.NotFound(fmt.Sprintf("graph: node %q output not found", nodeID))
	}
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	coerced, err := action.Coerce[T](value)
	if err != nil {
		return zero, xerr.Validation(
			fmt.Sprintf("graph: node %q output has type %T, expected %T", nodeID, value, zero))
	}
	return coerced, nil
}
