package action

import (
	"context"
	"fmt"
	"slices"

	"github.com/nexssp/kernel/xerr"
)

// StateEntity defines an interface for structs that possess a lifecycle state.
type StateEntity interface {
	GetState() string
	SetState(string)
}

// StateMachineBuilder provides a fluent DSL for building state-guarded actions.
type StateMachineBuilder[Req StateEntity, Res any] struct {
	name        string
	transitions map[string][]string // map[FromState]AllowedToStates
	exec        Fn[Req, Res]
}

// NewStateMachine creates an action that strictly guards state transitions.
func NewStateMachine[Req StateEntity, Res any](name string, exec Fn[Req, Res]) *StateMachineBuilder[Req, Res] {
	return &StateMachineBuilder[Req, Res]{
		name:        name,
		transitions: make(map[string][]string),
		exec:        exec,
	}
}

// Allow maps a valid state transition.
// Example: .Allow("pending", "paid", "canceled")
func (sm *StateMachineBuilder[Req, Res]) Allow(from string, to ...string) *StateMachineBuilder[Req, Res] {
	sm.transitions[from] = append(sm.transitions[from], to...)
	return sm
}

// Build compiles the State Machine into a standard Nexss Builder,
// injecting the validation middleware automatically.
func (sm *StateMachineBuilder[Req, Res]) Build() *BuiltAction[Req, Res] {
	b := New(sm.name, sm.exec)

	// Inject the state-guard middleware
	b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			currentState := req.GetState()

			// If we need to know the *target* state, we either infer it from the action
			// or the handler applies it. The guard ensures that if the state *was* changed
			// by the handler, it was a legal move.

			// We run the handler first to see the mutated state
			res, err := next(ctx, req)
			if err != nil {
				return res, err
			}

			newState := req.GetState()
			if currentState == newState {
				return res, nil // No transition occurred
			}

			allowed, exists := sm.transitions[currentState]
			if !exists {
				return res, xerr.Conflict(fmt.Sprintf("invalid transition from '%s'", currentState))
			}

			valid := slices.Contains(allowed, newState)

			if !valid {
				return res, xerr.Conflict(fmt.Sprintf("invalid transition: '%s' -> '%s'", currentState, newState))
			}

			return res, nil
		}
	})

	return b.Build()
}
