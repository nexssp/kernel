package action_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestAdaptive_OpenAndHalfOpen(t *testing.T) {
	t.Parallel()
	var fail atomic.Bool
	fail.Store(true)

	act := action.New("cb.test", func(ctx context.Context, req string) (string, error) {
		if fail.Load() {
			return "", xerr.Internal("boom") // Must return error to trip CB
		}
		return "ok", nil
	}).Use(action.Adaptive[string, string]("cb.test", action.AdaptiveConfig{
		FailureThreshold: 1,
		ResetTimeout:     100 * time.Millisecond,
		InitialTimeout:   50 * time.Millisecond,
	})).Build()

	// 1. Trigger Failure
	_, err := act.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected error")
	}

	// 2. Check Open state
	_, err = act.Do(context.Background(), "req")
	if xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("expected circuit breaker error, got kind: %v", xerr.From(err))
	}

	// 3. Recover
	fail.Store(false)
	time.Sleep(150 * time.Millisecond)

	res, err := act.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if res != "ok" {
		t.Fail()
	}
}

func TestAdaptive_Timeout(t *testing.T) {
	t.Parallel()
	// handler takes longer than current timeout, CB should apply timeout
	act := action.New("cb.timeout", func(ctx context.Context, _ string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return "slow", nil
		}
	}).Use(action.Adaptive[string, string]("cb.timeout", action.AdaptiveConfig{
		InitialTimeout: 10 * time.Millisecond,
	})).Build()

	_, err := act.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}
