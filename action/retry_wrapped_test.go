package action_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestRetry_WrappedTransientError(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	act := action.New("test.retry.wrapped", func(ctx context.Context, req struct{}) (string, error) {
		if attempts.Add(1) < 3 {
			// Wrap a transient xerr inside a plain fmt error
			return "", fmt.Errorf("db wrapper: %w", xerr.Unavailable("temporary glitch"))
		}
		return "success", nil
	}).Retry(3, action.ConstantBackoff(1*time.Millisecond)).Build()

	res, err := act.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "success" {
		t.Fatalf("expected 'success', got %v", res)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts (retry on wrapped transient), got %d", attempts.Load())
	}
}

func TestRetry_WrappedPermanentErrorNoRetry(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	act := action.New("test.retry.wrapped.permanent", func(ctx context.Context, req struct{}) (string, error) {
		attempts.Add(1)
		return "", fmt.Errorf("wrap: %w", xerr.Forbidden("nope"))
	}).Retry(3, action.ConstantBackoff(1*time.Millisecond)).Build()

	if _, err := act.Do(context.Background(), struct{}{}); err == nil {
		t.Fatal("expected error")
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected 1 attempt (no retry on permanent), got %d", attempts.Load())
	}
}
