package action_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type stateSnapshotEntity struct{ state string }

func (e *stateSnapshotEntity) GetState() string      { return e.state }
func (e *stateSnapshotEntity) SetState(state string) { e.state = state }

func TestStateMachineBuilderSnapshotsTransitions(t *testing.T) {
	builder := action.NewStateMachine("snapshot.typed", func(_ context.Context, entity *stateSnapshotEntity) (*stateSnapshotEntity, error) {
		entity.SetState("canceled")
		return entity, nil
	}).Allow("pending", "paid")
	built := builder.Build()
	builder.Allow("pending", "canceled")

	entity := &stateSnapshotEntity{state: "pending"}
	if _, err := built.Do(context.Background(), entity); xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("transition added after Build() error = %v, want Conflict", err)
	}
	if entity.GetState() != "pending" {
		t.Fatalf("rejected transition left state %q, want pending", entity.GetState())
	}
}

func TestStateMachineAnySnapshotsTransitions(t *testing.T) {
	get := func(value any) string {
		request, ok := value.(map[string]any)
		if !ok {
			return ""
		}
		state, _ := request["state"].(string)
		return state
	}
	set := func(value any, state string) {
		if request, ok := value.(map[string]any); ok {
			request["state"] = state
		}
	}
	transitions := map[string][]string{"pending": {"paid"}}
	inner := action.New("snapshot.inner", func(_ context.Context, request any) (any, error) {
		value, ok := request.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected request type %T", request)
		}
		value["state"] = "canceled"
		return value, nil
	}).Build()
	built := action.StateMachineAny("snapshot.dynamic", get, set, transitions, inner).Build()
	transitions["pending"] = append(transitions["pending"], "canceled")

	request := map[string]any{"state": "pending"}
	if _, err := built.Do(context.Background(), request); xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("transition added after construction error = %v, want Conflict", err)
	}
	if got := get(request); got != "pending" {
		t.Fatalf("rejected dynamic transition left state %q, want pending", got)
	}
}
