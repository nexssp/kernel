package action_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestBuiltAction_Do_LifecycleOrder(t *testing.T) {
	t.Parallel()

	var steps []string
	act := action.New("order.test", func(_ context.Context, req string) (string, error) {
		steps = append(steps, "handler")
		return "result_" + req, nil
	}).
		HookBefore(func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
			steps = append(steps, "before")
			return ctx, nil
		}).
		HookAfter(func(_ context.Context, _, _ string, _ error, _ *action.Meta) {
			steps = append(steps, "after")
		}).
		HookExecuted(func(_ context.Context, _, _ string, _ *action.Meta) {
			steps = append(steps, "executed")
		}).
		Build()

	res, err := act.Do(context.Background(), "input")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "result_input" {
		t.Fatalf("expected 'result_input', got %q", res)
	}

	expectedSteps := []string{"before", "handler", "executed", "after"}
	if len(steps) != len(expectedSteps) {
		t.Fatalf("expected steps %v, got %v", expectedSteps, steps)
	}
	for i, step := range steps {
		if step != expectedSteps[i] {
			t.Errorf("step %d: expected %q, got %q", i, expectedSteps[i], step)
		}
	}
}

func TestBuiltAction_Do_ErrorLifecycle(t *testing.T) {
	t.Parallel()

	var errorHookCalled, executedHookCalled bool
	act := action.New("error.test", func(_ context.Context, _ string) (string, error) {
		return "", errors.New("business failure")
	}).
		HookError(func(_ context.Context, _ string, _ error, _ *action.Meta) {
			errorHookCalled = true
		}).
		HookExecuted(func(_ context.Context, _, _ string, _ *action.Meta) {
			executedHookCalled = true
		}).
		Build()

	_, err := act.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errorHookCalled {
		t.Fatal("expected HookError to be called")
	}
	if executedHookCalled {
		t.Fatal("HookExecuted must not be called when execution fails")
	}
}

func TestBuiltAction_Do_PanicRecovery(t *testing.T) {
	t.Parallel()

	var panicHookRan atomic.Bool
	act := action.New("panic.test", func(_ context.Context, _ string) (string, error) {
		panic("fatal unexpected crash")
	}).
		AnyHook(action.AnyHook{
			OnPanic: func(_ context.Context, _, _ any, _ *action.Meta) {
				panicHookRan.Store(true)
			},
		}).
		Build()

	res, err := act.Do(context.Background(), "req")
	if res != "" {
		t.Fatalf("expected empty result on panic, got %q", res)
	}
	if err == nil {
		t.Fatal("expected error from recovered panic, got nil")
	}

	var appErr *xerr.AppError
	if !errors.As(err, &appErr) || appErr.Kind != xerr.KindInternal {
		t.Fatalf("expected xerr.KindInternal, got %v", err)
	}
	if !panicHookRan.Load() {
		t.Fatal("expected AnyHook.OnPanic to execute")
	}
}

func TestBuiltAction_ExecuteDecoded(t *testing.T) {
	t.Parallel()

	type RequestDto struct {
		Name string `json:"name"`
	}
	type ResponseDto struct {
		Greeting string `json:"greeting"`
	}

	act := action.New("decode.test", func(_ context.Context, req RequestDto) (ResponseDto, error) {
		return ResponseDto{Greeting: "Hello, " + req.Name}, nil
	}).Build()

	decodeFunc := func(target any) error {
		req, ok := target.(*RequestDto)
		if !ok {
			return errors.New("invalid target type")
		}
		req.Name = "Tester"
		return nil
	}

	rawRes, err := act.ExecuteDecoded(context.Background(), decodeFunc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	res, ok := rawRes.(ResponseDto)
	if !ok || res.Greeting != "Hello, Tester" {
		t.Fatalf("unexpected result: %+v", rawRes)
	}
}
