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

// TestResilient_BreakerObservesWholeCallNotEachAttempt is the regression test
// for moving Adaptive onto UseFirst inside Resilient(). Before the fix,
// Adaptive sat inside Retry, so AllowRequest/Observe fired once per retry
// attempt instead of once per external Do() call. A call that keeps failing
// transiently must exhaust every one of its own attempts against the real
// handler, and the breaker must see exactly one aggregate outcome per call.
func TestResilient_BreakerObservesWholeCallNotEachAttempt(t *testing.T) {
	t.Parallel()

	var handlerCalls atomic.Int32

	act := action.New("resilient.breaker.wraps.retry", func(_ context.Context, _ struct{}) (string, error) {
		handlerCalls.Add(1)
		return "", xerr.Unavailable("upstream flaking")
	}).Resilient(action.ResilienceConfig{
		MaxRetries: 3, // 4 attempts total per external call
		Backoff:    action.ConstantBackoff(time.Millisecond),
		Adaptive: &action.AdaptiveConfig{
			FailureThreshold: 2, // would trip mid-loop if Observe fired per attempt
			ResetTimeout:     time.Minute,
			InitialTimeout:   time.Minute,
		},
	}).Build()

	// If Adaptive were still inside Retry, the breaker would trip after the
	// 2nd real attempt and the 3rd/4th would be blocked before reaching the
	// handler — this call would finish early with a CircuitBreaker error
	// and only 2 handler invocations, not 4.
	_, err := act.Do(context.Background(), struct{}{})

	if got := handlerCalls.Load(); got != 4 {
		t.Fatalf("handler called %d times, want 4 (all attempts of one call must reach the handler)", got)
	}
	if xerr.KindFrom(err) == xerr.KindCircuitBreaker {
		t.Fatalf("breaker tripped mid-call: got CircuitBreaker, want the original transient failure: %v", err)
	}

	// One failed call = ONE aggregate failure against FailureThreshold=2,
	// so the breaker must still be closed for a second call.
	handlerCalls.Store(0)
	_, err2 := act.Do(context.Background(), struct{}{})
	if got := handlerCalls.Load(); got != 4 {
		t.Fatalf("2nd call: handler called %d times, want 4 (breaker must still be closed)", got)
	}
	if xerr.KindFrom(err2) == xerr.KindCircuitBreaker {
		t.Fatalf("breaker incorrectly open after only 1 aggregate failure: %v", err2)
	}

	// The 2nd call pushes the aggregate count to 2 -> threshold reached.
	// The 3rd external call must be rejected immediately, no handler call.
	handlerCalls.Store(0)
	_, err3 := act.Do(context.Background(), struct{}{})
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("3rd call: handler called %d times, want 0 (breaker must reject before the handler runs)", got)
	}
	if xerr.KindFrom(err3) != xerr.KindCircuitBreaker {
		t.Fatalf("3rd call: expected CircuitBreaker once threshold reached, got: %v", err3)
	}
}

// TestResilient_BreakerNeverSeesTransientHiccupsSwallowedByRetry proves the
// complementary invariant: a handler that fails a couple of times but then
// succeeds within the retry budget must never register a breaker failure —
// Observe(nil) fires exactly once for the whole call.
func TestResilient_BreakerNeverSeesTransientHiccupsSwallowedByRetry(t *testing.T) {
	t.Parallel()

	var attempt atomic.Int32

	act := action.New("resilient.breaker.recovers.midloop", func(_ context.Context, _ struct{}) (string, error) {
		if attempt.Add(1) <= 2 {
			return "", xerr.Unavailable("flaky upstream")
		}
		return "ok", nil
	}).Resilient(action.ResilienceConfig{
		MaxRetries: 3,
		Backoff:    action.ConstantBackoff(time.Millisecond),
		Adaptive: &action.AdaptiveConfig{
			FailureThreshold: 1, // would trip on the very first individual failure
			ResetTimeout:     time.Minute,
			InitialTimeout:   time.Minute,
		},
	}).Build()

	res, err := act.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("expected eventual success within the retry budget, got: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}

	// The call succeeded overall, so the breaker must stay closed for the
	// next call even though FailureThreshold is 1 and two attempts failed
	// along the way inside that first call.
	attempt.Store(2) // next call succeeds on its first try
	res2, err2 := act.Do(context.Background(), struct{}{})
	if err2 != nil {
		t.Fatalf("breaker incorrectly tripped from retry-internal failures: %v", err2)
	}
	if res2 != "ok" {
		t.Fatalf("expected 'ok', got %q", res2)
	}
}

// TestResilient_ConcurrencyLimitHoldsSlotAcrossRetries proves MaxConcurrent
// wraps the entire retry loop, not just a single attempt. A call that is
// sleeping in backoff between attempts must still occupy its concurrency
// slot for the whole time; a concurrent caller must be rejected during that
// gap, not only while the handler itself is executing.
func TestResilient_ConcurrencyLimitHoldsSlotAcrossRetries(t *testing.T) {
	t.Parallel()

	var handlerCalls atomic.Int32
	attemptDone := make(chan struct{}, 8)

	act := action.New("resilient.concurrency.wraps.retry", func(_ context.Context, _ struct{}) (string, error) {
		handlerCalls.Add(1)
		attemptDone <- struct{}{}
		return "", xerr.Unavailable("slow flaky upstream")
	}).Resilient(action.ResilienceConfig{
		MaxRetries:    2, // 3 attempts total with a real sleep between each
		Backoff:       action.ConstantBackoff(200 * time.Millisecond),
		MaxConcurrent: 1,
	}).Build()

	done := make(chan error, 1)
	go func() {
		_, err := act.Do(context.Background(), struct{}{})
		done <- err
	}()

	// Wait for the first attempt to finish. The long-running call is now
	// asleep in backoff, about to make its second attempt.
	select {
	case <-attemptDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first attempt")
	}

	// Fire a concurrent call *during the backoff gap*. If ConcurrencyLimit
	// were re-acquired per attempt (the old bug), this gap is exactly when
	// the slot would be free and this call would wrongly succeed.
	_, errConcurrent := act.Do(context.Background(), struct{}{})
	if !errors.Is(errConcurrent, action.ErrConcurrencyLimit) {
		t.Fatalf("expected ErrConcurrencyLimit while the first call is still retrying, got: %v", errConcurrent)
	}

	if err := <-done; err == nil {
		t.Fatal("expected the long-running call to eventually fail (handler never succeeds)")
	}
	if got := handlerCalls.Load(); got != 3 {
		t.Fatalf("handler called %d times, want 3 (all attempts of the long-running call)", got)
	}

	// The slot must be released once the long-running call fully finishes.
	handlerCalls.Store(0)
	_, err := act.Do(context.Background(), struct{}{})
	if errors.Is(err, action.ErrConcurrencyLimit) {
		t.Fatal("concurrency slot was not released after the first call fully completed")
	}
	if got := handlerCalls.Load(); got == 0 {
		t.Fatal("expected the handler to run once the slot was freed")
	}
}
