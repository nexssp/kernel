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

	act := action.New("smart.retry", func(ctx context.Context, req string) (string, error) {
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

	act := action.New("smart.noretry", func(ctx context.Context, req string) (string, error) {
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
