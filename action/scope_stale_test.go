// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Regression tests for correctness audit fixes: circuit breaker panic
// paths, idempotency conflict/corruption/lease-renewal, saga partial
// rollback, dedup/history panics, state-machine pre-validation.

package action_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// ─── CircuitBreaker: panic paths ─────────────────────────────────────────────

// PANIC DURING HALF-OPEN PROBE: before the fix, the panic unwound past
// Observe/ReleaseProbe, leaving the breaker in stateHalfOpen forever and
// rejecting every future request until process restart.
func TestAdaptive_PanicDuringHalfOpenProbe_DoesNotWedgeBreaker(t *testing.T) {
	t.Parallel()

	var fail atomic.Bool
	var panicArmed atomic.Bool
	fail.Store(true)

	probeAct := action.New("cb.panic.probe", func(_ context.Context, _ string) (string, error) {
		if panicArmed.Load() {
			panicArmed.Store(false)
			panic("probe panic")
		}
		if fail.Load() {
			return "", xerr.Internal("boom")
		}
		return "ok", nil
	}).Use(action.Adaptive[string, string]("cb.panic.probe", action.AdaptiveConfig{
		FailureThreshold: 1,
		ResetTimeout:     80 * time.Millisecond,
		InitialTimeout:   50 * time.Millisecond,
	})).Build()

	// 1. Trip the breaker with a normal failure.
	if _, err := probeAct.Do(context.Background(), "req"); err == nil {
		t.Fatal("expected failure to trip breaker")
	}
	// 2. Sanity: breaker open.
	if _, err := probeAct.Do(context.Background(), "req"); xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("expected breaker open, got: %v", err)
	}

	// 3. Wait out ResetTimeout; arm the panic for the half-open probe call.
	time.Sleep(120 * time.Millisecond)
	panicArmed.Store(true)
	// Do() recovers the panic into an error — the breaker must still observe it.
	_, _ = probeAct.Do(context.Background(), "req")

	// 4. The breaker must NOT be stuck in half-open: after ResetTimeout a new
	// probe is admitted (it fails with "boom", NOT with CircuitBreaker).
	// Before the fix, AllowRequest returned false forever at this point.
	time.Sleep(120 * time.Millisecond)
	_, err := probeAct.Do(context.Background(), "req")
	if xerr.KindFrom(err) == xerr.KindCircuitBreaker {
		t.Fatal("breaker wedged after panicking probe: request rejected while a probe window was due")
	}
	if err == nil {
		t.Fatal("expected the failing probe's error")
	}

	// 5. Full recovery: next window, handler succeeds → breaker closes.
	time.Sleep(120 * time.Millisecond)
	fail.Store(false)
	res, err := probeAct.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("expected full recovery after panicking probe, got: %v", err)
	}
	if res != "ok" {
		t.Fatalf("unexpected result: %q", res)
	}
}

// PANIC IN CLOSED STATE: panics count as failures and trip the breaker like
// any other error (previously they bypassed Observe entirely).
func TestAdaptive_PanicInClosedState_TripsBreaker(t *testing.T) {
	t.Parallel()

	calls := 0
	act := action.New("cb.panic.closed", func(_ context.Context, _ string) (string, error) {
		calls++
		panic("always panics")
	}).Use(action.Adaptive[string, string]("cb.panic.closed", action.AdaptiveConfig{
		FailureThreshold: 3,
		ResetTimeout:     time.Second,
		InitialTimeout:   50 * time.Millisecond,
	})).Build()

	var lastErr error
	for range 3 {
		_, lastErr = act.Do(context.Background(), "req")
	}
	if lastErr == nil {
		t.Fatal("expected errors from panicking calls")
	}

	// Breaker must now be OPEN: next call rejected without invoking handler.
	before := calls
	_, err := act.Do(context.Background(), "req")
	if xerr.KindFrom(err) != xerr.KindCircuitBreaker {
		t.Fatalf("expected breaker to trip on panics, got: %v", err)
	}
	if calls != before {
		t.Fatal("handler ran while breaker should be open")
	}
}

// ─── Idempotency: concurrent payload mismatch ────────────────────────────────

// CONCURRENT KEY REUSE WITH DIFFERENT PAYLOAD: caller B joins caller A's
// singleflight (same key) while A is in flight. B must receive a Conflict,
// not A's response. Before the fix the follower path skipped the hash check
// entirely and silently returned A's response for a different body.
func TestIdempotency_ConcurrentDifferentPayload_ReturnsConflict(t *testing.T) {
	t.Parallel()

	handlerEntered := make(chan struct{})
	release := make(chan struct{})
	var closeOnce sync.Once

	cfg := action.IdempotencyConfig{
		Enabled:   true,
		KeyFunc:   func(_ []byte) string { return "shared-key" },
		KeyHeader: "X-Key",
	}

	mw := action.IdempotencyMiddleware[[]byte, string](nil, cfg)
	core := func(_ context.Context, _ []byte) (string, error) {
		closeOnce.Do(func() { close(handlerEntered) })
		<-release
		return "leader-result", nil
	}
	wrapped := mw(core)

	results := make(chan error, 2)

	// Leader: payload "A" (hash H_A), key "shared-key".
	go func() {
		_, err := wrapped(context.Background(), []byte("A"))
		results <- err
	}()

	<-handlerEntered // leader holds the flight

	// Follower: payload "B" (hash H_B), same key — joins the in-flight
	// leader via singleflight and never runs the flight function itself.
	go func() {
		_, err := wrapped(context.Background(), []byte("B"))
		results <- err
	}()

	// Give the follower a moment to join, then let the leader finish.
	time.Sleep(50 * time.Millisecond)
	close(release)

	errA := <-results
	errB := <-results
	if errA != nil {
		t.Fatalf("leader should succeed, got: %v", errA)
	}
	if xerr.KindFrom(errB) != xerr.KindConflict {
		t.Fatalf("follower with different payload must get Conflict, got: %v", errB)
	}
}

// CORRUPTED STORED ENTRY: a stored response that no longer unmarshals must
// fail closed instead of re-executing the handler (double side effects).
func TestIdempotency_CorruptedEntry_FailsClosed(t *testing.T) {
	t.Parallel()

	store := action.NewMemoryIdempotencyStore(time.Minute)
	// Pre-seed an entry whose body is not valid JSON for the response type.
	store.Set(context.Background(), "k-corrupt", action.IdempotencyEntry{
		Status:      200,
		Body:        []byte("{not-json"),
		StoredAt:    time.Now().UTC(),
		RequestHash: "", // empty hash disables mismatch checks
	}, time.Minute)

	cfg := action.IdempotencyConfig{
		Enabled:   true,
		KeyHeader: "X-Key",
	}
	mw := action.IdempotencyMiddleware[string, string](store, cfg)

	executions := 0
	core := func(_ context.Context, _ string) (string, error) {
		executions++
		return "should-not-run", nil
	}
	wrapped := mw(core)

	// Key via request scope RequestID so we hit the seeded entry.
	ctx := context.Background()
	ctx = xctx.WithRequestID(ctx, "k-corrupt")

	_, err := wrapped(ctx, "req")
	if err == nil {
		t.Fatal("expected error for corrupted entry")
	}
	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected internal error, got: %v", err)
	}
	if executions != 0 {
		t.Fatalf("handler re-executed for corrupted entry (%d times)", executions)
	}
}

// LEASE RENEWAL: a handler running longer than LeaseTTL must still Complete
// successfully because the middleware renews the claim while it runs.
// Without renewal the lease expires mid-flight and Complete fails with
// "no active claim", turning a committed side effect into an error.
func TestIdempotency_LeaseRenewal_KeepsClaimAlive(t *testing.T) {
	t.Parallel()

	leaseTTL := 80 * time.Millisecond
	store := action.NewMemoryIdempotencyStore(time.Minute)
	cfg := action.IdempotencyConfig{
		Enabled:   true,
		KeyHeader: "X-Key",
		LeaseTTL:  leaseTTL,
		Store:     store,
	}
	mw := action.IdempotencyMiddleware[string, string](store, cfg)

	core := func(_ context.Context, _ string) (string, error) {
		// Outlive the lease several times over; renewal interval is ~26ms.
		time.Sleep(300 * time.Millisecond)
		return "slow-but-done", nil
	}
	wrapped := mw(core)

	ctx := xctx.WithRequestID(context.Background(), "k-renew")
	res, err := wrapped(ctx, "req")
	if err != nil {
		t.Fatalf("expected Complete to succeed with renewal, got: %v", err)
	}
	if res != "slow-but-done" {
		t.Fatalf("unexpected result: %q", res)
	}

	// The completed entry must now replay for the same key.
	res2, err := wrapped(ctx, "req")
	if err != nil {
		t.Fatalf("expected replay, got: %v", err)
	}
	if res2 != "slow-but-done" {
		t.Fatalf("replay mismatch: %q", res2)
	}
}

// ─── Saga: success path leaves no compensation artifacts ─────────────────────

func TestSaga_Success_NoCompensationErrors(t *testing.T) {
	t.Parallel()

	saga := action.NewSaga[string, string]("saga.clean").
		AddStep("s1",
			func(_ context.Context, _ string) (string, error) { return "ok", nil },
			func(_ context.Context, _ string) error { return nil }).
		Build()

	res, err := saga.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if res.RolledBack {
		t.Fatal("successful saga must not be marked RolledBack")
	}
	if len(res.CompensationErrors) != 0 {
		t.Fatalf("successful saga must have no compensation errors, got: %v", res.CompensationErrors)
	}
}

// ─── Deduplicate: leader sees its own panic ──────────────────────────────────

// LEADER PANIC: the leader's caller previously received (zero, nil) — the
// panic error was only delivered to followers via the shared inflight call.
func TestDeduplicate_LeaderPanic_ReturnsError(t *testing.T) {
	t.Parallel()

	mw := action.Deduplicate[string, string](func(req string) string {
		if req == "" {
			return ""
		}
		return "k"
	})

	core := func(_ context.Context, _ string) (string, error) {
		panic("dedup boom")
	}
	wrapped := mw(core, nil) // hooks unused on the error path

	_, err := wrapped(context.Background(), "req")
	if err == nil {
		t.Fatal("leader must see the panic as an error, got nil")
	}
	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected internal error, got: %v", err)
	}
}

// ─── History: panicking calls are recorded ───────────────────────────────────

func TestHistory_PanickingCall_IsRecorded(t *testing.T) {
	t.Parallel()

	hist := action.NewHistory[string, string](16)
	mw := action.HistoryMiddleware[string, string](hist)

	core := func(_ context.Context, _ string) (string, error) {
		panic("history boom")
	}
	wrapped := mw(core)

	// BuiltAction.Do recovers panics into errors; call the middleware chain
	// through the middleware directly and recover here.
	_, _ = func() (res string, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("recovered: %v", r)
			}
		}()
		return wrapped(context.Background(), "req")
	}()

	snap := hist.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected exactly 1 history record, got %d", len(snap))
	}
	if snap[0].Err == nil {
		t.Fatal("recorded entry must carry the panic error")
	}
}

// ─── StateMachine: pre-validation before side effects ────────────────────────

type auditFixEntity struct {
	State string
}

func (e *auditFixEntity) GetState() string  { return e.State }
func (e *auditFixEntity) SetState(s string) { e.State = s }

func TestStateMachine_PreValidation_RejectsBeforeSideEffects(t *testing.T) {
	t.Parallel()

	handlerRan := 0
	handler := func(_ context.Context, e *auditFixEntity) (*auditFixEntity, error) {
		handlerRan++
		e.SetState("paid") // simulate side effects (DB writes etc.)
		return e, nil
	}

	sm := action.NewStateMachine("order.auditfix", handler).
		Allow("pending", "paid").
		Build()

	e := &auditFixEntity{State: "shipped"} // "shipped" has no outbound transitions
	_, err := sm.Do(context.Background(), e)
	if xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("expected conflict for unmapped source state, got: %v", err)
	}
	if handlerRan != 0 {
		t.Fatalf("handler ran (%d times) for a request that can never be valid", handlerRan)
	}
	if e.State != "shipped" {
		t.Fatalf("entity must be untouched, got %q", e.State)
	}
}
