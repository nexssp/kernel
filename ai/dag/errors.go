package dag

import (
	"errors"
	"fmt"
)

// ErrSuspended signals that a node paused execution (e.g. awaiting human
// approval). Returning it preserves partial state for later resume.
var ErrSuspended = errors.New("graph: execution suspended for human intervention")

// SuspendError carries the reason and payload for a graceful pause.
// Reason is human-readable; Payload is what the approver needs to see.
type SuspendError struct {
	Reason  string
	Payload any
}

func (e *SuspendError) Error() string        { return "graph suspended: " + e.Reason }
func (e *SuspendError) Is(target error) bool { return target == ErrSuspended || target == e }

// Suspend pauses DAG execution gracefully. The returned error matches
// ErrSuspended.
func Suspend(reason string, payload any) error {
	return &SuspendError{Reason: reason, Payload: payload}
}

// ExecutionError reports that a layer stopped before every node in it
// succeeded. State carries the partial result: every node whose output is
// present already ran, so a caller that persists State and later
// re-executes with it will re-run only the missing nodes.
//
// The caller owns State and must call Release on it, exactly as with a
// successful return.
type ExecutionError struct {
	Layer      int
	FailedNode string
	Cause      error
	State      *State
	Completed  []string
}

func (e *ExecutionError) Error() string {
	if e.FailedNode == "" {
		return fmt.Sprintf("graph execution failed at layer %d: %v", e.Layer, e.Cause)
	}
	return fmt.Sprintf("graph execution failed at node %q (layer %d): %v",
		e.FailedNode, e.Layer, e.Cause)
}

func (e *ExecutionError) Unwrap() error { return e.Cause }
