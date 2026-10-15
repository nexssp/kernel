package action_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func newTestBreaker(t *testing.T, key string, reset time.Duration) *action.CircuitBreakerState {
	t.Helper()
	state, err := action.NewCircuitBreakerState(key, action.CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     reset,
	})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCircuitBreaker_StateTransitionsAndPermanentErrors(t *testing.T) {
	t.Parallel()
	var fail atomic.Bool
	fail.Store(true)
	state := newTestBreaker(t, "payments", 25*time.Millisecond)
	act := action.New("payments.call", func(_ context.Context, _ struct{}) (string, error) {
		if fail.Load() {
			return "", xerr.Unavailable("upstream down")
		}
		return "ok", nil
	}).Use(action.CircuitBreaker[struct{}, string](state)).Build()

	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindUnavailable || !xerr.IsTransient(err) {
		t.Fatalf("first call error = %v, want Unavailable", err)
	}
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindCircuitBreaker || !xerr.IsTransient(err) {
		t.Fatalf("open call error = %v, want CircuitBreaker", err)
	}

	// A permanent client error neither trips nor closes a breaker.
	permanentState := newTestBreaker(t, "permanent", time.Second)
	permanentAct := action.New("permanent.call", func(context.Context, struct{}) (string, error) {
		return "", xerr.Forbidden("denied")
	}).Use(action.CircuitBreaker[struct{}, string](permanentState)).Build()
	for range 3 {
		if _, err := permanentAct.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindForbidden || !xerr.IsPermanent(err) {
			t.Fatalf("permanent failure = %v, want Forbidden", err)
		}
	}

	time.Sleep(35 * time.Millisecond)
	fail.Store(false)
	got, err := act.Do(t.Context(), struct{}{})
	if err != nil || got != "ok" {
		t.Fatalf("half-open probe = (%q, %v), want (ok, nil)", got, err)
	}
	if _, err := act.Do(t.Context(), struct{}{}); err != nil {
		t.Fatalf("breaker did not close after successful probe: %v", err)
	}
}

func TestCircuitBreaker_OnlyOneHalfOpenProbe(t *testing.T) {
	t.Parallel()
	var mode atomic.Int32
	probeEntered := make(chan struct{})
	probeRelease := make(chan struct{})
	state := newTestBreaker(t, "probe", 20*time.Millisecond)
	act := action.New("probe.call", func(_ context.Context, _ struct{}) (string, error) {
		switch mode.Load() {
		case 0:
			return "", xerr.Unavailable("down")
		case 1:
			close(probeEntered)
			<-probeRelease
			return "ok", nil
		default:
			return "ok", nil
		}
	}).Use(action.CircuitBreaker[struct{}, string](state)).Build()
	mode.Store(0)
	_, _ = act.Do(t.Context(), struct{}{})
	time.Sleep(30 * time.Millisecond)
	mode.Store(1)
	probeDone := make(chan error, 1)
	go func() { _, err := act.Do(t.Context(), struct{}{}); probeDone <- err }()
	select {
	case <-probeEntered:
	case <-time.After(time.Second):
		t.Fatal("half-open probe did not enter handler")
	}
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("concurrent half-open call = %v, want CircuitBreaker", err)
	}
	close(probeRelease)
	if err := <-probeDone; err != nil {
		t.Fatalf("probe failed: %v", err)
	}
}

func TestCircuitBreaker_CanceledProbeCanRetryLater(t *testing.T) {
	t.Parallel()
	var mode atomic.Int32
	probeEntered := make(chan struct{})
	state := newTestBreaker(t, "cancel-probe", 20*time.Millisecond)
	act := action.New("cancel-probe.call", func(ctx context.Context, _ struct{}) (string, error) {
		switch mode.Load() {
		case 0:
			return "", xerr.Unavailable("down")
		case 1:
			close(probeEntered)
			<-ctx.Done()
			return "", xerr.Canceled("probe canceled", ctx.Err())
		default:
			return "ok", nil
		}
	}).Use(action.CircuitBreaker[struct{}, string](state)).Build()
	mode.Store(0)
	_, _ = act.Do(t.Context(), struct{}{})
	time.Sleep(30 * time.Millisecond)
	mode.Store(1)
	ctx, cancel := context.WithCancel(t.Context())
	probeDone := make(chan error, 1)
	go func() { _, err := act.Do(ctx, struct{}{}); probeDone <- err }()
	select {
	case <-probeEntered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("canceled probe did not enter handler")
	}
	cancel()
	select {
	case err := <-probeDone:
		if xerr.KindFrom(err) != xerr.KindCanceled {
			t.Fatalf("canceled probe error = %v, want Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled probe did not return")
	}
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("breaker should reopen after canceled probe, got %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	mode.Store(2)
	if got, err := act.Do(t.Context(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("re-probe = (%q, %v), want (ok, nil)", got, err)
	}
}

func TestCircuitBreaker_PanicDuringProbeDoesNotWedgeState(t *testing.T) {
	t.Parallel()
	var mode atomic.Int32
	state := newTestBreaker(t, "panic-probe", 20*time.Millisecond)
	act := action.New("panic-probe.call", func(context.Context, struct{}) (string, error) {
		switch mode.Load() {
		case 0:
			return "", xerr.Unavailable("down")
		case 1:
			panic("probe panic")
		default:
			return "ok", nil
		}
	}).Use(action.CircuitBreaker[struct{}, string](state)).Build()
	mode.Store(0)
	_, _ = act.Do(t.Context(), struct{}{})
	time.Sleep(30 * time.Millisecond)
	mode.Store(1)
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("panic recovery error = %v, want Internal", err)
	}
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("breaker should reopen after probe panic, got %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	mode.Store(2)
	if got, err := act.Do(t.Context(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("re-probe = (%q, %v), want (ok, nil)", got, err)
	}
}

func TestCircuitBreakerRegistry_IsolationAndConfigMismatch(t *testing.T) {
	t.Parallel()
	cfg := action.CircuitBreakerConfig{FailureThreshold: 2, ResetTimeout: time.Second}
	first := action.NewCircuitBreakerRegistry()
	second := action.NewCircuitBreakerRegistry()
	state1, err := first.GetOrCreate("shared", cfg)
	if err != nil {
		t.Fatal(err)
	}
	same, err := first.GetOrCreate("shared", cfg)
	if err != nil || same != state1 {
		t.Fatalf("same registry returned state %p, %v; want %p, nil", same, err, state1)
	}
	isolated, err := second.GetOrCreate("shared", cfg)
	if err != nil || isolated == state1 {
		t.Fatalf("separate registry reused shared state: (%p, %v)", isolated, err)
	}
	if _, err := first.GetOrCreate("shared", action.CircuitBreakerConfig{FailureThreshold: 3, ResetTimeout: time.Second}); xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("configuration mismatch error = %v, want Conflict", err)
	}
	if _, err := first.GetOrCreate(" ", cfg); xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("invalid key error = %v, want Validation", err)
	}
}

func TestCircuitBreakerConfigValidationAndZeroValue(t *testing.T) {
	t.Parallel()
	for _, cfg := range []action.CircuitBreakerConfig{
		{FailureThreshold: -1},
		{ResetTimeout: -time.Second},
	} {
		if _, err := action.NewCircuitBreakerState("invalid", cfg); xerr.KindFrom(err) != xerr.KindValidation {
			t.Fatalf("invalid config error = %v, want Validation", err)
		}
	}
	var zero action.CircuitBreakerState
	act := action.New("zero", func(context.Context, struct{}) (string, error) { return "ok", nil }).
		Use(action.CircuitBreaker[struct{}, string](&zero)).Build()
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("zero-value breaker error = %v, want Validation", err)
	}

	builder := action.New("invalid-resilience", func(context.Context, struct{}) (string, error) { return "ok", nil })
	if err := builder.ApplyResilience(action.ResilienceConfig{MaxRetries: -1}); xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("checked resilience config error = %v, want Validation", err)
	}
	// Failed validation must be atomic: a valid configuration can still be applied.
	if err := builder.ApplyResilience(action.ResilienceConfig{}); err != nil {
		t.Fatalf("valid resilience after failed validation: %v", err)
	}
	if err := builder.ApplyResilience(action.ResilienceConfig{}); xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("second ApplyResilience error = %v, want Conflict", err)
	}

	fluent := action.New("invalid-fluent", func(context.Context, struct{}) (string, error) { return "ok", nil }).
		Resilient(action.ResilienceConfig{Timeout: -time.Second}).Build()
	if _, err := fluent.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("fluent invalid configuration error = %v, want Validation", err)
	}
}

func TestCircuitBreakerAllowsUnnamedStatesButRegistryRequiresKey(t *testing.T) {
	t.Parallel()
	cfg := action.CircuitBreakerConfig{FailureThreshold: 2, ResetTimeout: time.Second}
	state, err := action.NewCircuitBreakerState("", cfg)
	if err != nil {
		t.Fatalf("unnamed standalone state: %v", err)
	}
	act := action.New("unnamed", func(context.Context, struct{}) (string, error) { return "ok", nil }).
		Use(action.CircuitBreaker[struct{}, string](state)).Build()
	if got, err := act.Do(t.Context(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("unnamed standalone breaker = (%q, %v), want (ok, nil)", got, err)
	}

	adaptive := action.New("", func(context.Context, struct{}) (string, error) { return "ok", nil }).
		Use(action.Adaptive[struct{}, string]("", action.AdaptiveConfig{})).Build()
	if got, err := adaptive.Do(t.Context(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("unnamed Adaptive breaker = (%q, %v), want (ok, nil)", got, err)
	}

	if _, err := action.NewCircuitBreakerRegistry().GetOrCreate("", cfg); err == nil {
		t.Fatal("registry accepted an empty breaker key")
	}
}

func TestApplyResilience_CircuitBreakerWrapsRetryAndTimeoutIsPerAttempt(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	state := newTestBreaker(t, "wraps-retry", time.Second)
	act := action.New("wraps-retry", func(context.Context, struct{}) (string, error) {
		calls.Add(1)
		return "", xerr.Unavailable("down")
	})
	if err := act.ApplyResilience(action.ResilienceConfig{
		MaxRetries:     2,
		Backoff:        action.ConstantBackoff(time.Nanosecond),
		CircuitBreaker: state,
	}); err != nil {
		t.Fatal(err)
	}
	built := act.Build()
	if _, err := built.Do(t.Context(), struct{}{}); err == nil {
		t.Fatal("expected final retry failure")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler called %d times, want 3; breaker must wrap the full retry loop", got)
	}
	if _, err := built.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("next logical call = %v, want CircuitBreaker", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("open breaker allowed handler call: count %d", got)
	}

	var timeoutCalls atomic.Int32
	timed := action.New("per-attempt-timeout", func(ctx context.Context, _ struct{}) (string, error) {
		timeoutCalls.Add(1)
		<-ctx.Done()
		return "", ctx.Err()
	})
	if err := timed.ApplyResilience(action.ResilienceConfig{
		MaxRetries: 1,
		Backoff:    action.ConstantBackoff(time.Nanosecond),
		Predicate:  func(error) bool { return true },
		Timeout:    15 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := timed.Build().Do(t.Context(), struct{}{})
	if err == nil || timeoutCalls.Load() != 2 {
		t.Fatalf("per-attempt timeout calls=%d err=%v, want 2 attempts and an error", timeoutCalls.Load(), err)
	}
}

func TestCircuitBreaker_InternalErrorsTrip(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	state := newTestBreaker(t, "unclassified", time.Second)
	act := action.New("unclassified.call", func(context.Context, struct{}) (string, error) {
		calls.Add(1)
		return "", xerr.Internal("local processing failure")
	}).Use(action.CircuitBreaker[struct{}, string](state)).Build()

	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindInternal ||
		xerr.IsTransient(err) || xerr.IsPermanent(err) {
		t.Fatalf("internal failure = %v, want Internal and neither transient nor permanent", err)
	}
	if _, err := act.Do(t.Context(), struct{}{}); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("call after Internal failure = %v, want CircuitBreaker", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1 after breaker opened", got)
	}
}
