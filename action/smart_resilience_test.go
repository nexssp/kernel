package action_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestSmartResilience_RetryTransient(t *testing.T) {
	t.Parallel()
	var attempt atomic.Int32

	act := action.New("smart.retry", func(_ context.Context, _ string) (string, error) {
		a := attempt.Add(1)
		if a < 3 {
			return "", xerr.Unavailable("transient")
		}
		return "success", nil
	}).InferredResilient().Build()

	res, err := act.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "success" {
		t.Fatalf("expected 'success', got %q", res)
	}
	if attempt.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempt.Load())
	}
}

func TestSmartResilience_NoRetryOnBadRequest(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32

	act := action.New("smart.noretry", func(_ context.Context, _ string) (string, error) {
		callCount.Add(1)
		return "", xerr.BadRequest("invalid")
	}).InferredResilient().Build()

	_, err := act.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount.Load() != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", callCount.Load())
	}
}

func TestSmartResilience_ForwardsRetryHooks(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	var retryHookCalls atomic.Int32

	act := action.New("smart.retry.hooks", func(_ context.Context, _ string) (string, error) {
		if attempts.Add(1) < 3 {
			return "", xerr.Unavailable("transient")
		}
		return "ok", nil
	}).
		InferredResilient().
		HookRetry(func(_ context.Context, _ string, _ int, _ error, _ *action.Meta) {
			retryHookCalls.Add(1)
		}).
		Build()

	res, err := act.Do(context.Background(), "req")
	if err != nil || res != "ok" {
		t.Fatalf("res=%q err=%v", res, err)
	}
	if got := retryHookCalls.Load(); got != 2 {
		t.Fatalf("OnRetry hook fired %d times, want 2 (regression: SmartResilience dropped the dispatcher)", got)
	}
}
