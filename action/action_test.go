package action_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestExecuteDecoded_PointerRequest(t *testing.T) {
	type Req struct{ A int }
	act := action.New("test", func(ctx context.Context, req *Req) (int, error) {
		return req.A, nil
	}).Build()
	res, err := act.ExecuteDecoded(context.Background(), func(v any) error {
		req, ok := v.(*Req)
		if !ok {
			return fmt.Errorf("expected *Req, got %T", v)
		}
		*req = Req{A: 42}
		return nil
	})
	if err != nil || res != 42 {
		t.Fail()
	}
}

func TestExecuteDecoded_ValueRequest(t *testing.T) {
	t.Parallel()
	type Req struct{ A int } // Value type, not pointer
	act := action.New("test.value", func(ctx context.Context, req Req) (int, error) {
		return req.A, nil
	}).Build()

	res, err := act.ExecuteDecoded(context.Background(), func(v any) error {
		// v is passed as a pointer to the value type by ExecuteDecoded
		req, ok := v.(*Req)
		if !ok {
			return fmt.Errorf("expected *Req, got %T", v)
		}
		*req = Req{A: 99}
		return nil
	})
	if err != nil || res != 99 {
		t.Fatalf("expected 99, got %v (err: %v)", res, err)
	}
}

func TestRace_AllFailures(t *testing.T) {
	t.Parallel()
	act := action.New("race.fail", func(ctx context.Context, req int) (int, error) {
		return 0, errors.New("always fails")
	}).Build()

	res, err := action.Race(context.Background(), act, []int{1, 2})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != 0 {
		t.Fatalf("expected zero value, got %d", res)
	}
}

func TestAction_ContextCancelTriggersOnCancel(t *testing.T) {
	t.Parallel()
	var canceled bool

	act := action.New("test.cancel", func(ctx context.Context, req int) (int, error) {
		return 0, context.Canceled
	}).Hook(action.Hook[int, int]{
		OnCancel: func(ctx context.Context, req int, m *action.Meta) {
			canceled = true
		},
		OnError: func(ctx context.Context, req int, err error, m *action.Meta) {
			t.Fatal("OnError should not be called when context is canceled")
		},
	}).Build()

	_, err := act.Do(context.Background(), 1)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}

	if !canceled {
		t.Fatal("expected OnCancel to be called")
	}
}
