package action_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
)

type handlerErrorEntity struct {
	state string
	note  string
}

func (e *handlerErrorEntity) GetState() string      { return e.state }
func (e *handlerErrorEntity) SetState(state string) { e.state = state }

func TestStateMachine_RestoresOnlyStateWhenHandlerErrors(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("handler failed")
	machine := action.NewStateMachine("error.rollback.typed", func(_ context.Context, entity *handlerErrorEntity) (*handlerErrorEntity, error) {
		entity.SetState("failed")
		entity.note = "handler mutation remains"
		return entity, handlerErr
	}).Allow("pending", "paid").Build()
	entity := &handlerErrorEntity{state: "pending", note: "before"}

	_, err := machine.Do(context.Background(), entity)
	if !errors.Is(err, handlerErr) {
		t.Fatalf("handler error = %v, want original error", err)
	}
	if entity.GetState() != "pending" {
		t.Fatalf("state = %q, want restored state %q", entity.GetState(), "pending")
	}
	if entity.note != "handler mutation remains" {
		t.Fatalf("non-state mutation = %q, want it preserved", entity.note)
	}
}

func TestStateMachineAny_RestoresOnlyStateWhenHandlerErrors(t *testing.T) {
	t.Parallel()

	handlerErr := errors.New("dynamic handler failed")
	getState := func(value any) string {
		entity, ok := value.(map[string]any)
		if !ok {
			return ""
		}
		state, _ := entity["state"].(string)
		return state
	}
	setState := func(value any, state string) {
		if entity, ok := value.(map[string]any); ok {
			entity["state"] = state
		}
	}
	inner := action.New("error.rollback.dynamic.handler", func(_ context.Context, request any) (any, error) {
		entity, ok := request.(map[string]any)
		if !ok {
			return nil, errors.New("unexpected request type")
		}
		entity["state"] = "failed"
		entity["note"] = "handler mutation remains"
		return entity, handlerErr
	}).Build()
	machine := action.StateMachineAny(
		"error.rollback.dynamic",
		getState,
		setState,
		map[string][]string{"pending": {"paid"}},
		inner,
	).Build()
	request := map[string]any{"state": "pending", "note": "before"}

	_, err := machine.Do(context.Background(), request)
	if !errors.Is(err, handlerErr) {
		t.Fatalf("handler error = %v, want original error", err)
	}
	if got := request["state"]; got != "pending" {
		t.Fatalf("state = %v, want restored state %q", got, "pending")
	}
	if got := request["note"]; got != "handler mutation remains" {
		t.Fatalf("non-state mutation = %v, want it preserved", got)
	}
}
