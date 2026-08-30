package action_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestSaga_Success(t *testing.T) {
	t.Parallel()

	saga := action.NewSaga[int, int]("order.saga").
		AddStep("step1", func(ctx context.Context, req int) (int, error) {
			return req + 10, nil
		}, nil).
		AddStep("step2", func(ctx context.Context, req int) (int, error) {
			return req * 2, nil
		}, nil).
		Build()

	res, err := saga.Do(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Success || res.Output != 10 {
		t.Fatalf("expected success with output 10, got: %+v", res)
	}
}

func TestSaga_RollbackLIFO(t *testing.T) {
	t.Parallel()

	var undo1Ran, undo2Ran atomic.Bool

	saga := action.NewSaga[string, string]("payment.saga").
		AddStep("reserve_funds",
			func(ctx context.Context, req string) (string, error) { return "reserved", nil },
			func(ctx context.Context, req string) error { undo1Ran.Store(true); return nil },
		).
		AddStep("authorize_card",
			func(ctx context.Context, req string) (string, error) { return "authorized", nil },
			func(ctx context.Context, req string) error { undo2Ran.Store(true); return nil },
		).
		AddStep("failing_step",
			func(ctx context.Context, req string) (string, error) { return "", errors.New("terminal bank failure") },
			nil,
		).
		Build()

	res, err := saga.Do(context.Background(), "order_123")
	if err == nil {
		t.Fatal("expected saga to fail")
	}
	if !res.RolledBack {
		t.Fatal("expected saga to be marked as RolledBack")
	}
	if !undo1Ran.Load() || !undo2Ran.Load() {
		t.Fatalf("expected both undos to run, got undo1=%v, undo2=%v", undo1Ran.Load(), undo2Ran.Load())
	}
}

func TestSaga_UndoPanicRecovery(t *testing.T) {
	t.Parallel()

	var undo1Ran atomic.Bool
	var undo2Ran atomic.Bool

	saga := action.NewSaga[string, string]("resilient.saga").
		AddStep("step_1",
			func(ctx context.Context, req string) (string, error) { return "ok", nil },
			func(ctx context.Context, req string) error {
				undo1Ran.Store(true)
				return nil
			},
		).
		AddStep("step_2",
			func(ctx context.Context, req string) (string, error) { return "ok", nil },
			func(ctx context.Context, req string) error {
				undo2Ran.Store(true)
				panic("step 2 compensation panic")
			},
		).
		AddStep("step_3",
			func(ctx context.Context, req string) (string, error) {
				return "", errors.New("terminal failure in step 3")
			},
			nil,
		).
		Build()

	res, err := saga.Do(context.Background(), "req")

	if err == nil {
		t.Fatal("expected saga failure")
	}
	if !res.RolledBack {
		t.Fatal("expected saga to be marked as RolledBack")
	}

	if !undo2Ran.Load() {
		t.Error("step 2 undo did not run")
	}
	if !undo1Ran.Load() {
		t.Error("step 1 undo did not run after step 2 panic")
	}
}
