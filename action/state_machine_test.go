package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

type Order struct {
	ID    string
	State string
}

func (o *Order) GetState() string  { return o.State }
func (o *Order) SetState(s string) { o.State = s }

func TestStateMachine(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, o *Order) (*Order, error) {
		o.SetState("paid")
		return o, nil
	}

	sm := action.NewStateMachine("order.pay", handler).
		Allow("pending", "paid").
		Allow("failed", "paid").
		Build() // Returns *Builder, so we Build() twice

	// Valid transition
	o1 := &Order{State: "pending"}
	_, err := sm.Do(context.Background(), o1)
	if err != nil {
		t.Fatalf("unexpected error on valid transition: %v", err)
	}

	// Invalid transition
	o2 := &Order{State: "shipped"}
	_, err = sm.Do(context.Background(), o2)
	if err == nil {
		t.Fatal("expected conflict error on invalid transition, got nil")
	}
}
