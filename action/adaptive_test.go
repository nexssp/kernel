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

	act := action.New("cb.test", func(_ context.Context, _ string) (string, error) {
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

func TestAdaptive_DoesNotTripOnForbidden(t *testing.T) {
	t.Parallel()

	act := action.New("cb.no4xx", func(_ context.Context, _ string) (string, error) {
		return "", xerr.Forbidden("permission denied")
	}).Use(action.Adaptive[string, string]("cb.no4xx", action.AdaptiveConfig{
		FailureThreshold: 1,
		ResetTimeout:     50 * time.Millisecond,
		InitialTimeout:   50 * time.Millisecond,
	})).Build()

	// Even after many 4xx responses, the CB must remain closed.
	for range 10 {
		_, err := act.Do(context.Background(), "req")
		if err == nil {
			t.Fatal("expected Forbidden error, got nil")
		}
		if xerr.KindFrom(err) != xerr.KindForbidden {
			t.Fatalf("expected Forbidden, got %v", err)
		}
	}
}

// TestAdaptive_CancelledProbe_LeavesBreakerPermanentlyHalfOpen reproduces
// the liveness bug: a probe that is aborted by the caller's context
// cancellation never reaches state.Observe, so the breaker stays in
// stateHalfOpen forever and rejects every future request, healthy or not.
func TestAdaptive_CancelledProbe_LeavesBreakerPermanentlyHalfOpen(t *testing.T) {
	t.Parallel()

	var mode atomic.Int32
	// 0 = return a hard failure (used to trip the breaker)
	// 1 = block until ctx is done (used to simulate a probe that is canceled)
	// 2 = succeed (used to prove the breaker recovers)

	probeEntered := make(chan struct{})

	act := action.New("cb.halfopen.cancel", func(ctx context.Context, _ string) (string, error) {
		switch mode.Load() {
		case 0:
			return "", xerr.Internal("upstream down")
		case 1:
			close(probeEntered)
			<-ctx.Done()
			return "", ctx.Err()
		default:
			return "ok", nil
		}
	}).Use(action.Adaptive[string, string]("cb.halfopen.cancel", action.AdaptiveConfig{
		FailureThreshold: 1,
		ResetTimeout:     30 * time.Millisecond,
		InitialTimeout:   30 * time.Millisecond,
	})).Build()

	// Phase 1: trip the breaker (Closed -> Open).
	mode.Store(0)
	if _, err := act.Do(context.Background(), "req"); err == nil {
		t.Fatal("expected first upstream failure to trip the breaker")
	}
	if _, err := act.Do(context.Background(), "req"); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("expected Open state, got %v", err)
	}

	// Phase 2: wait out ResetTimeout so the next call is treated as a probe.
	time.Sleep(50 * time.Millisecond)

	// Phase 3: fire the probe, then cancel its context mid-flight.
	mode.Store(1)
	probeCtx, cancel := context.WithCancel(context.Background())
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		_, _ = act.Do(probeCtx, "req")
	}()

	<-probeEntered // deterministic wait!
	cancel()
	<-probeDone

	// Phase 4: the breaker must recover. With the fix, ReleaseProbe puts it
	// back into Open so the next ResetTimeout window re-arms a fresh probe.
	time.Sleep(50 * time.Millisecond)

	mode.Store(2)
	res, err := act.Do(context.Background(), "req")
	if xerr.KindFrom(err) == xerr.KindCircuitBreaker {
		t.Fatal("BUG: circuit breaker permanently stuck in stateHalfOpen after a canceled probe; " +
			"every subsequent call is rejected regardless of upstream health")
	}
	if err != nil {
		t.Fatalf("unexpected error after recovery window: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}
}
