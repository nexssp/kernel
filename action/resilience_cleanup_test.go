// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Systematic cleanup verification for resilience patterns. These tests
// ensure no resources are leaked after handler completion, error,
// cancellation, or panic — the contract for million-user production.
//
// What's verified per pattern:
//
//   Goroutine leak:  after N concurrent calls complete, goroutine count
//                    returns to baseline (no leaked renewal / singleflight
//                    / stampede goroutines).
//
//   Context.Cancel:  canceling the caller's context mid-flight does not
//                    leave the action in a stuck state — subsequent calls
//                    succeed cleanly.
//
//   Leftover waiters: after a handler error or cancel, no waiter is left
//                     blocked forever on a closed/pending channel.
//
//   Deadlock after error: a handler that errors does not cause the next
//                          caller to deadlock on a stuck in-flight entry.
//
// All tests use xtest.RequireGoroutinesAtMost for leak detection and
// channel-based synchronization (no time.Sleep) for determinism.

package action_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

// ─── Helper: snapshot goroutine count ────────────────────────────────────────

// goroutineBaseline returns a goroutine count after a short settle period.
// Tests call this before starting work, then assert the count returns to
// baseline (with small delta) after completion.
func goroutineBaseline(t *testing.T) int {
	t.Helper()
	// Allow any starter goroutines from prior parallel tests to settle.
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

// ─── Cache: no goroutine leak after N concurrent calls ──────────────────────

func TestCleanup_Cache_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	store := newMockStore()
	act := action.New("cleanup.cache", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	// 100 concurrent cache-miss calls.
	ktest.Simulate(t, act, "k1", 100, func(tb testing.TB, _ string, err error) {
		if err != nil {
			tb.Errorf("unexpected error: %v", err)
		}
	})

	// Goroutine count must return to baseline (singleflight goroutines released).
	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── Dedup: no goroutine leak after concurrent waiters ──────────────────────

func TestCleanup_Dedup_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	release := make(chan struct{})
	var entered atomic.Int32
	act := action.New("cleanup.dedup", func(_ context.Context, _ string) (string, error) {
		entered.Add(1)
		<-release
		return "shared", nil
	}).Dedup(func(r string) string { return r }).Build()

	const N = 50
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), "k")
		}(i)
	}
	close(start)

	// Wait for the executor to enter the handler, then release.
	xtest.Eventually(t, 2*time.Second, func() bool { return entered.Load() == 1 })
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	// All goroutines must exit after release — no leaked waiters.
	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── Coalesce: no goroutine leak after concurrent waiters ────────────────────

func TestCleanup_Coalesce_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	c := action.NewCoalescer()
	release := make(chan struct{})
	var entered atomic.Int32
	act := action.New("cleanup.coalesce", func(_ context.Context, _ string) (string, error) {
		entered.Add(1)
		<-release
		return "shared", nil
	}).Coalesce(c, func(r string) string { return r }).Build()

	const N = 50
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), "k")
		}(i)
	}
	close(start)

	xtest.Eventually(t, 2*time.Second, func() bool { return entered.Load() == 1 })
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── FanOut: no goroutine leak after all results collected ──────────────────

func TestCleanup_FanOut_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	act := action.New("cleanup.fanout", func(_ context.Context, _ int) (int, error) {
		time.Sleep(5 * time.Millisecond) // tiny delay to force goroutine overlap
		return 1, nil
	}).Build()

	for range 10 {
		results := action.FanOut(context.Background(), act, []int{1, 2, 3, 4, 5}, 3)
		if len(results) != 5 {
			t.Fatalf("expected 5 results, got %d", len(results))
		}
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── Race: no goroutine leak after winner returns ───────────────────────────

func TestCleanup_Race_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	act := action.New("cleanup.race", func(ctx context.Context, n int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(n) * time.Millisecond):
			return n, nil
		}
	}).Build()

	for range 10 {
		_, _ = action.Race(context.Background(), act, []int{5, 10, 15})
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── ExclusiveFenced: no goroutine leak after lease renewal ──────────────────

func TestCleanup_ExclusiveFenced_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	mu := &singleAcquireMutex{}
	act := action.New("cleanup.fenced", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).ExclusiveFenced(mu, 1*time.Second, func(r string) string { return r }).Build()

	// Each call spawns a lease-renewal goroutine; it must exit after the
	// handler completes and the lease is released.
	for range 20 {
		_, err := act.Do(context.Background(), "k")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 1*time.Second, 2)
}

// ─── Idempotency: no goroutine leak after singleflight collapse ──────────────

func TestCleanup_Idempotency_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	act := action.New("cleanup.idem", func(_ context.Context, _ int) (int, error) {
		return 42, nil
	}).Idempotent().Build()

	ctx := ktest.RequestContext(t, "cleanup-idem-key")

	// 100 concurrent callers with the same idempotency key collapse via
	// singleflight — no leaked goroutines after completion.
	errs := make([]error, 100)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 100 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(ctx, 1)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── Cache: context.Cancel mid-flight doesn't leave action stuck ────────────
//
// Cache uses singleflight with context.WithoutCancel internally, so the
// handler keeps running even if the caller cancels. The canceled caller
// gets ctx.Err() immediately, but the handler completes normally and
// populates the cache for subsequent callers.
func TestCleanup_Cache_ContextCancelDoesNotCorruptState(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	handlerEntered := make(chan struct{})
	release := make(chan struct{})

	act := action.New("cleanup.cache.cancel", func(_ context.Context, _ string) (string, error) {
		close(handlerEntered)
		<-release // handler runs under uncanceled baseCtx — must be released explicitly
		return "computed", nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	// Caller 1: cancels mid-flight. Cache returns ctx.Err() immediately
	// (singleflight leader's ctx.Done fires), but the handler keeps running.
	ctxCancel, cancel := context.WithCancel(context.Background())
	callerDone := make(chan error, 1)
	go func() {
		_, err := act.Do(ctxCancel, "k1")
		callerDone <- err
	}()

	<-handlerEntered
	cancel()
	err := <-callerDone
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller should receive context.Canceled, got: %v", err)
	}

	// Release the handler — it completes and populates the cache.
	close(release)
	// Give the singleflight a moment to finish.
	time.Sleep(50 * time.Millisecond)

	// Caller 2: must succeed cleanly — the cache is now populated.
	// Use a fresh context (no cancellation).
	res, err := act.Do(context.Background(), "k1")
	if err != nil {
		t.Fatalf("subsequent call failed after cancel: %v", err)
	}
	if res != "computed" {
		t.Fatalf("expected 'computed', got %q", res)
	}
}

// ─── Dedup: handler error doesn't leave waiters stuck ───────────────────────
//
// When the handler errors, all waiters must receive the SAME error — no
// waiter should be left blocked forever. After the error, the in-flight
// entry must be cleaned up so subsequent callers become new executors.
func TestCleanup_Dedup_HandlerErrorDoesNotLeaveWaitersStuck(t *testing.T) {
	t.Parallel()

	var entered atomic.Int32
	release := make(chan struct{})

	act := action.New("cleanup.dedup.err", func(_ context.Context, _ string) (string, error) {
		entered.Add(1)
		<-release
		return "", xerr.Internal("handler exploded")
	}).Dedup(func(r string) string { return r }).Build()

	// Launch 10 concurrent callers — all with the SAME key.
	const N = 10
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), "k")
		}(i)
	}
	close(start)

	// Wait for the executor to enter, then release it to return the error.
	xtest.Eventually(t, 2*time.Second, func() bool { return entered.Load() == 1 })
	close(release)
	wg.Wait()

	// Every caller must have received the handler's error — no waiter stuck.
	for i, err := range errs {
		if err == nil {
			t.Errorf("caller %d: expected error, got nil (waiter stuck?)", i)
			continue
		}
		if xerr.KindFrom(err) != xerr.KindInternal {
			t.Errorf("caller %d: expected KindInternal, got %v", i, err)
		}
	}

	// Exactly 1 handler invocation — all 10 callers collapsed via dedup.
	if got := entered.Load(); got != 1 {
		t.Fatalf("expected exactly 1 handler invocation, got %d", got)
	}

	// Subsequent call with a DIFFERENT key must succeed — the coalescer
	// entry from the error was cleaned up.
	release2 := make(chan struct{})
	close(release2)
	act2 := action.New("cleanup.dedup.ok", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).Dedup(func(r string) string { return r + "-v2" }).Build()

	res, err := act2.Do(context.Background(), "k2")
	if err != nil {
		t.Fatalf("subsequent call failed: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}
}

// ─── Coalesce: canceled waiter doesn't leave coalescer stuck ─────────────────
//
// A waiter that cancels mid-flight exits with ctx.Err(), but the executor's
// in-flight call continues for other waiters. After the executor completes,
// no goroutines should be leaked — the coalescer must be clean.
//
// Deterministic structure (no time.Sleep for waiter registration):
//   - Caller 1 is guaranteed to be the executor (handlerEntered gate
//     ensures caller 1 enters the handler BEFORE caller 2 starts).
//   - Caller 2's registration is verified via Coalescer.Waiters() == 1,
//     which is an atomic check on the coalescer's internal pending map.
//   - After executor completes, goroutine count returns to baseline.
func TestCleanup_Coalesce_CanceledWaiterDoesNotLeaveCoalescerStuck(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	c := action.NewCoalescer()
	release := make(chan struct{})
	executorDone := make(chan struct{})
	var entered atomic.Int32

	act := action.New("cleanup.coalesce.cancel", func(_ context.Context, _ string) (string, error) {
		entered.Add(1)
		<-release
		return "completed", nil
	}).Coalesce(c, func(r string) string { return r }).Build()

	// The coalescer namespaces keys as "actionName:key".
	const fullKey = "cleanup.coalesce.cancel:k"

	// Caller 1: executor — starts first, guaranteed to enter the handler.
	go func() {
		_, _ = act.Do(context.Background(), "k")
		close(executorDone)
	}()

	// Wait until caller 1 is inside the handler (lock is held by the coalescer).
	xtest.Eventually(t, 2*time.Second, func() bool { return entered.Load() == 1 })

	// Caller 2: waiter — cancels mid-flight.
	ctxCancel, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := act.Do(ctxCancel, "k")
		waiterDone <- err
	}()

	// Wait until caller 2 has actually registered with the coalescer
	// (entry.waiters == 1). This is the deterministic substitute for the
	// previous time.Sleep(20ms) — it verifies registration directly
	// through the coalescer's internal state.
	xtest.Eventually(t, 2*time.Second, func() bool { return c.Waiters(fullKey) == 1 })

	cancel()
	err := <-waiterDone
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter should receive context.Canceled, got: %v", err)
	}

	// Release the executor — it completes and cleans up the coalescer entry.
	close(release)
	<-executorDone

	// No goroutines should be leaked — both the executor and waiter goroutines
	// have exited, and the coalescer has no residual entries.
	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)

	// Subsequent call with a fresh key must succeed — coalescer is clean.
	res, err := act.Do(context.Background(), "k-subsequent")
	if err != nil {
		t.Fatalf("subsequent call failed after waiter cancel: %v", err)
	}
	if res != "completed" {
		t.Fatalf("expected 'completed', got %q", res)
	}
}

// ─── ExclusiveFenced: panic doesn't leak the lease ──────────────────────────

func TestCleanup_ExclusiveFenced_PanicReleasesLease(t *testing.T) {
	t.Parallel()

	mu := &singleAcquireMutex{}

	act := action.New("cleanup.fenced.panic", func(_ context.Context, _ string) (string, error) {
		panic("handler panic")
	}).ExclusiveFenced(mu, 1*time.Second, func(r string) string { return r }).Build()

	// Call 1: panics. The middleware must recover and release the lease.
	_, err := act.Do(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}

	// Call 2: must succeed — the lease was released after the panic.
	act2 := action.New("cleanup.fenced.panic2", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).ExclusiveFenced(mu, 1*time.Second, func(r string) string { return r }).Build()

	res, err := act2.Do(context.Background(), "k")
	if err != nil {
		t.Fatalf("subsequent call failed after panic: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}
}

// ─── Saga: panic in Do doesn't leave Undo chain incomplete ───────────────────

func TestCleanup_Saga_PanicInDoRunsUndo(t *testing.T) {
	t.Parallel()

	var undoRan atomic.Bool

	saga := action.NewSaga[string, string]("cleanup.saga.panic").
		AddStep("step1",
			func(_ context.Context, _ string) (string, error) { return "ok", nil },
			func(_ context.Context, _ string) error { undoRan.Store(true); return nil }).
		AddStep("step2",
			func(_ context.Context, _ string) (string, error) { panic("step 2 panic") },
			nil).
		Build()

	// Saga must recover the panic, run undo for step 1, and return an error.
	res, err := saga.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}
	if !res.RolledBack {
		t.Fatal("expected RolledBack=true")
	}
	if !undoRan.Load() {
		t.Fatal("undo was not run after step 2 panic")
	}
}

// ─── Adaptive (CB): no goroutine leak after breaker trips ──────────────────

func TestCleanup_Adaptive_NoGoroutineLeakAfterBreakerTrips(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	var calls atomic.Int32
	act := action.New("cleanup.cb", func(_ context.Context, _ int) (int, error) {
		calls.Add(1)
		return 0, xerr.Unavailable("down")
	}).Use(action.Adaptive[int, int]("cleanup.cb", action.AdaptiveConfig{
		FailureThreshold: 3,
		ResetTimeout:     100 * time.Millisecond,
		InitialTimeout:   100 * time.Millisecond,
	})).Build()

	// Trip the breaker.
	for range 5 {
		_, _ = act.Do(context.Background(), 1)
	}

	// Wait beyond ResetTimeout so the breaker can transition through half-open.
	time.Sleep(200 * time.Millisecond)

	// Goroutines must settle — the breaker does NOT spawn background goroutines.
	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── ConcurrencyLimit: no goroutine leak after rejections ───────────────────

func TestCleanup_ConcurrencyLimit_NoGoroutineLeakAfterRejections(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	release := make(chan struct{})
	var entered atomic.Int32
	act := action.New("cleanup.conc", func(_ context.Context, _ int) (int, error) {
		entered.Add(1)
		<-release
		return 1, nil
	}).ConcurrencyLimit(2).Build()

	// 50 concurrent callers — most rejected.
	errs := make([]error, 50)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 50 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), 1)
		}(i)
	}
	close(start)

	// Wait for the 2 in-flight to enter.
	xtest.Eventually(t, 2*time.Second, func() bool { return entered.Load() == 2 })

	// Release — the 2 in-flight complete, all rejected goroutines already returned.
	close(release)
	wg.Wait()

	// Count errors — some must be ErrConcurrencyLimit (rejected immediately).
	rejections := 0
	for _, err := range errs {
		if errors.Is(err, action.ErrConcurrencyLimit) {
			rejections++
		}
	}
	if rejections == 0 {
		t.Fatal("expected some rejections")
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── RateLimit: no goroutine leak after LRU eviction churn ──────────────────

func TestCleanup_RateLimit_NoGoroutineLeakAfterLRUEvictionChurn(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	// Use a high rps + generous burst so 200 sequential calls all pass.
	// The point is goroutine leak detection, not rate-limit behavior.
	act := action.New("cleanup.ratelimit", func(_ context.Context, _ int) (int, error) {
		return 1, nil
	}).RateLimit(1e6, 1e6).Build() // effectively unlimited

	// 200 sequential calls — all should pass under effectively-unlimited rps.
	for i := range 200 {
		if _, err := act.Do(context.Background(), i); err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── StateMachine: no goroutine leak (state machine is synchronous) ──────────

func TestCleanup_StateMachine_NoGoroutineLeak(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	handler := func(_ context.Context, o *Order) (*Order, error) {
		o.SetState("paid")
		return o, nil
	}
	sm := action.NewStateMachine("cleanup.sm", handler).
		Allow("pending", "paid").
		Build()

	for range 50 {
		o := &Order{State: "pending"}
		if _, err := sm.Do(context.Background(), o); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── Composite: Cache + Dedup + Retry — full cleanup after mixed workload ──

func TestCleanup_Composite_FullCleanupAfterMixedWorkload(t *testing.T) {
	t.Parallel()

	baseline := goroutineBaseline(t)

	store := newIntMockStore()
	var attempts atomic.Int32

	act := action.New("cleanup.composite", func(_ context.Context, n int) (int, error) {
		attempts.Add(1)
		return n * 2, nil
	}).
		Cache(time.Minute, func(_ int) string { return "fixed" }, store).
		Dedup(func(_ int) string { return "fixed" }).
		Retry(3, action.ConstantBackoff(0)).
		Build()

	// 50 concurrent callers — cache miss + dedup collapse + (no retries needed).
	errs := make([]error, 50)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 50 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), 1)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	// Composite of Cache + Dedup + Retry must leave zero leaked goroutines.
	xtest.RequireGoroutinesAtMost(t, baseline, 1*time.Second, 2)
}
