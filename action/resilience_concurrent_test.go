// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// This file holds concurrent stress tests for resilience patterns that
// previously lacked them. The goal is to exercise each pattern under
// realistic concurrent load using xtest.Gate (start-barrier) and
// ktest.Simulate (bounded parallel Do) to surface data races.
//
// All tests are t.Parallel() and pass under `go test -race`.

package action_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

// ─── Retry: per-call retry budget is isolated across concurrent callers ────

// N concurrent callers each get their own retry budget. Each caller's handler
// fails on its first 2 attempts and succeeds on the 3rd. Total handler
// invocations must equal N × 3 (1 + 2 retries per caller). This actually
// exercises the retry path — unlike a "always succeed" test which only
// checks call count.
//
// Per-call attempt counter is keyed by request identity, so each goroutine's
// retry budget is independent — no cross-contamination.
func TestRetry_Concurrent_AttemptsAreIsolatedPerCall(t *testing.T) {
	t.Parallel()

	// Per-caller attempt counter: caller i's handler fails for attempts 0 and 1,
	// succeeds on attempt 2. Use a map keyed by request value so concurrent
	// callers don't share state.
	var (
		mu         sync.Mutex
		attempts   = make(map[int]int)
		totalCalls atomic.Int32
	)

	act := action.New("retry.concurrent", func(_ context.Context, req int) (int, error) {
		totalCalls.Add(1)
		mu.Lock()
		a := attempts[req]
		attempts[req] = a + 1
		mu.Unlock()
		if a < 2 {
			return 0, xerr.Unavailable("transient")
		}
		return req, nil
	}).Retry(3, action.ConstantBackoff(0)).Build()

	const N = 50
	errs := make([]error, N)
	results := make([]int, N)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = act.Do(context.Background(), idx)
		}(i)
	}
	close(start)
	wg.Wait()

	// Every caller must succeed and receive its own input back.
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
		if results[i] != i {
			t.Errorf("caller %d: expected result %d, got %d", i, i, results[i])
		}
	}

	// Each caller used exactly 3 attempts (1 + 2 retries).
	if got := totalCalls.Load(); got != N*3 {
		t.Fatalf("expected %d handler calls (N×3), got %d", N*3, got)
	}

	// Verify per-caller attempt counts — proves retry budget didn't leak across callers.
	mu.Lock()
	defer mu.Unlock()
	for i := range N {
		if attempts[i] != 3 {
			t.Fatalf("caller %d: expected 3 attempts, got %d (retry budget leaked)", i, attempts[i])
		}
	}
}

// ─── RateLimit: concurrent under-limit throughput ──────────────────────────

// 100 concurrent calls with rps=10000 + burst=10000 — all should pass.
// Verifies the atomic limiter has no contention-induced rejections.
func TestRateLimit_Concurrent_AllPassUnderHighRate(t *testing.T) {
	t.Parallel()

	act := action.New("ratelimit.concurrent", func(_ context.Context, _ int) (int, error) {
		return 1, nil
	}).RateLimit(10000, 10000).Build()

	const N = 200
	results := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, results[idx] = act.Do(context.Background(), idx)
		}(i)
	}
	close(start)
	wg.Wait()

	rejected := 0
	for _, e := range results {
		if e != nil {
			rejected++
		}
	}
	if rejected > 0 {
		t.Fatalf("expected 0 rejections under high rate, got %d", rejected)
	}
}

// ─── ConcurrencyLimit: max in-flight never exceeds limit ────────────────────

// With limit=3 and callers=50, many calls will be rejected — that's expected.
// What we verify is that max in-flight never exceeds the limit, AND that
// both successful and rejected calls behave correctly.
func TestConcurrencyLimit_Concurrent_NeverExceedsLimit(t *testing.T) {
	t.Parallel()

	const limit = 3
	const callers = 50

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	act := action.New("conc.concurrent", func(_ context.Context, _ int) (int, error) {
		current := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if current <= old || maxInFlight.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
		return 1, nil
	}).ConcurrencyLimit(limit).Build()

	var successes, rejections atomic.Int32
	ktest.Simulate(t, act, 1, callers, func(tb testing.TB, _ int, err error) {
		if err != nil {
			if !errors.Is(err, action.ErrConcurrencyLimit) {
				tb.Errorf("unexpected error: %v", err)
			}
			rejections.Add(1)
			return
		}
		successes.Add(1)
	})

	if peak := maxInFlight.Load(); peak > int32(limit) {
		t.Fatalf("max in-flight %d exceeded limit %d", peak, limit)
	}
	if successes.Load() == 0 {
		t.Fatal("expected at least one successful call")
	}
	if rejections.Load() == 0 {
		t.Fatal("expected some rejections given limit=3 and callers=50")
	}
}

// ─── Idempotency: 200 concurrent → exactly 1 handler invocation ─────────────

func TestIdempotency_Concurrent_200CallersOneExecution(t *testing.T) {
	t.Parallel()

	executions := ktest.NewCounter()
	const callers = 200

	act := action.New("idempotent.concurrent", func(_ context.Context, _ int) (int, error) {
		executions.Inc()
		time.Sleep(20 * time.Millisecond) // force overlap
		return 42, nil
	}).Idempotent().Build()

	// All callers share the same Idempotency-Key via request context.
	// ktest.Simulate uses context.Background() internally so we can't use it here.
	ctx := ktest.RequestContext(t, "concurrent-key-200")

	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(ctx, 1)
		}(i)
	}
	close(start)
	wg.Wait()

	// Every caller must succeed — if idempotency returned an error to ANY
	// caller, the test must fail (not just rely on executions.Require).
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	executions.Require(t, 1)
}

// ─── Cache: thundering-herd under concurrent misses ─────────────────────────

func TestCache_Concurrent_Stampede_CollapsedBySingleflight(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	executions := ktest.NewCounter()
	gate := make(chan struct{})

	act := action.New("cache.stampede", func(_ context.Context, _ string) (string, error) {
		executions.Inc()
		<-gate // block until test releases
		return "computed", nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	const N = 50
	results := make([]string, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{}) // start barrier — all callers arrive at tryLayers simultaneously

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			res, err := act.Do(context.Background(), "hot-key")
			results[idx] = res
			errs[idx] = err
		}(i)
	}
	close(start) // release all callers simultaneously

	// Wait for exactly one handler invocation to have entered.
	xtest.Eventually(t, 2*time.Second, func() bool { return executions.Load() == 1 })
	// Brief pause to let any remaining callers register with singleflight.
	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()

	// All callers should get the computed value.
	for i, r := range results {
		if errs[i] != nil {
			t.Errorf("caller %d: unexpected error: %v", i, errs[i])
		}
		if r != "computed" {
			t.Errorf("caller %d: expected 'computed', got %q", i, r)
		}
	}

	// Only ONE handler invocation should have occurred.
	executions.Require(t, 1)
}

// ─── Dedup: 200 concurrent → exactly 1 handler invocation ────────────────────

// THUNDERING HERD — proper version with deterministic release.
// Uses a keyFn counter to know when ALL 200 callers have entered act.Do
// before releasing the executor. This prevents late arrivals from
// becoming new executors after the first one finishes.
func TestDedup_Concurrent_200CallersOneExecution(t *testing.T) {
	t.Parallel()

	executions := ktest.NewCounter()
	var keyCalls atomic.Int32
	release := make(chan struct{})

	act := action.New("dedup.herd", func(_ context.Context, _ string) (string, error) {
		executions.Inc()
		<-release
		return "herd-result", nil
	}).Dedup(func(r string) string {
		keyCalls.Add(1) // incremented by EVERY caller (executor + waiters)
		return r
	}).Build()

	const N = 200
	done := make(chan struct {
		res string
		err error
	}, N)

	for range N {
		go func() {
			res, err := act.Do(context.Background(), "herd-key")
			done <- struct {
				res string
				err error
			}{res, err}
		}()
	}

	// Wait until all 200 callers have entered act.Do (via the keyFn counter).
	// This guarantees no late-arriving new executors.
	xtest.Eventually(t, 5*time.Second, func() bool { return keyCalls.Load() == N })

	// Now release — all 200 get the shared result.
	close(release)

	for range N {
		r := <-done
		if r.err != nil {
			t.Errorf("unexpected error: %v", r.err)
		}
		if r.res != "herd-result" {
			t.Errorf("expected 'herd-result', got %q", r.res)
		}
	}

	executions.Require(t, 1)
}

// ─── Coalesce: 200 concurrent across 2 actions sharing one Coalescer ───────
//
// Deterministic synchronization: instead of counting keyFn calls (which
// fire BEFORE the waiter registers with the coalescer — leaving a race
// window where the executor finishes before latecomers call Do), we
// poll Coalescer.Waiters() to confirm that all N-1 waiters per action
// are actually registered with the coalescer's internal pending map.
//
// Only then do we release the executors. This eliminates the flakiness
// that occurs when totalKeyCalls reaches N*2 but 3 callers are still
// between keyFn and coalescer.Do — they would become new executors.
func TestCoalesce_Concurrent_SharedCoalescerAcrossActions(t *testing.T) {
	t.Parallel()

	c := action.NewCoalescer()
	execA := ktest.NewCounter()
	execB := ktest.NewCounter()
	release := make(chan struct{})

	mkAct := func(name string, counter *ktest.Counter, out string) *action.BuiltAction[string, string] {
		return action.New(name, func(_ context.Context, _ string) (string, error) {
			counter.Inc()
			<-release
			return out, nil
		}).Coalesce(c, func(r string) string { return r }).Build()
	}

	actA := mkAct("coal.shared.a", execA, "from-a")
	actB := mkAct("coal.shared.b", execB, "from-b")

	// The coalescer namespaces keys as "actionName:key". Confirm these
	// full keys match the format used by CoalesceMiddleware.
	const keyA = "coal.shared.a:shared-key"
	const keyB = "coal.shared.b:shared-key"

	const N = 50
	var wg sync.WaitGroup
	errs := make([]error, N*2) // 50 actA + 50 actB callers

	// Start barrier — all callers arrive at CoalesceMiddleware simultaneously.
	start := make(chan struct{})
	for i := range N {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = actA.Do(context.Background(), "shared-key")
		}(i)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[N+idx] = actB.Do(context.Background(), "shared-key")
		}(i)
	}
	close(start)

	// Poll the coalescer directly: wait until both executors are inside the
	// handler AND all N-1 waiters per action have registered.
	// Each action's executor calls handler once; the remaining N-1 callers
	// become waiters (entry.waiters increments on the c.Do slow path).
	xtest.Eventually(t, 5*time.Second, func() bool {
		return execA.Load() == 1 && execB.Load() == 1 &&
			c.Waiters(keyA) == N-1 && c.Waiters(keyB) == N-1
	})

	// NOW release — all callers are deterministically registered.
	close(release)
	wg.Wait()

	// Every caller must succeed.
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	execA.Require(t, 1)
	execB.Require(t, 1)
}

// ─── SmartResilience: per-call retry budget isolation under load ──────────

// N concurrent callers each get their own retry budget. Each caller's handler
// fails on its first 2 attempts and succeeds on the 3rd. SmartResilience
// retries up to 3 times per caller invocation, so total handler invocations
// must equal N × 3. This actually exercises the retry path — not just the
// happy path.
//
// Crucially: the circuit breaker must NOT trip, because each caller-call
// ultimately succeeds. If the CB tripped, we'd see fewer than N×3 handler
// calls — a regression here means SmartResilience's failure accounting
// leaks across caller invocations.
func TestSmartResilience_Concurrent_PerCallRetryBudgetIsolated(t *testing.T) {
	t.Parallel()

	var (
		mu            sync.Mutex
		attempts      = make(map[int]int)
		totalAttempts atomic.Int32
	)

	act := action.New("smart.concurrent", func(_ context.Context, req int) (int, error) {
		totalAttempts.Add(1)
		mu.Lock()
		a := attempts[req]
		attempts[req] = a + 1
		mu.Unlock()
		if a < 2 {
			return 0, xerr.Unavailable("transient")
		}
		return req, nil
	}).InferredResilient().Build()

	const N = 30
	errs := make([]error, N)
	results := make([]int, N)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = act.Do(context.Background(), idx)
		}(i)
	}
	close(start)
	wg.Wait()

	// Every caller must succeed — CB did NOT trip, retries were isolated.
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error (CB likely tripped — retry budget leaked): %v", i, err)
		}
		if results[i] != i {
			t.Errorf("caller %d: expected result %d, got %d", i, i, results[i])
		}
	}

	// Each caller used exactly 3 attempts (1 + 2 retries).
	if got := totalAttempts.Load(); got != N*3 {
		t.Fatalf("expected %d handler calls (N×3), got %d — CB may have tripped", N*3, got)
	}

	// Per-caller attempt counts — proves retry budget didn't leak.
	mu.Lock()
	defer mu.Unlock()
	for i := range N {
		if attempts[i] != 3 {
			t.Fatalf("caller %d: expected 3 attempts, got %d (retry budget leaked)", i, attempts[i])
		}
	}
}

// ─── Adaptive (CB): two-phase deterministic breaker trip ────────────────────

// Phase 1 (synchronous): trip the breaker by making `FailureThreshold`
// sequential caller calls. After this phase, the breaker is GUARANTEED
// to be in `Open` state — no race possible.
//
// Phase 2 (concurrent): launch N concurrent callers. They MUST all be
// rejected by the open breaker with KindCircuitBreaker — zero handler
// invocations. This is the contract: once Open, the breaker fast-fails
// every caller without consulting the handler.
//
// Two-phase eliminates the flakiness of the previous version where
// concurrent goroutines raced the breaker's failure counter and could
// all see Closed state if scheduling allowed.
func TestAdaptive_Concurrent_OpenBreakerRejectsAll(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	act := action.New("cb.concurrent.fail", func(_ context.Context, _ int) (int, error) {
		calls.Add(1)
		return 0, xerr.Unavailable("down")
	}).Use(action.Adaptive[int, int]("cb.concurrent.fail", action.AdaptiveConfig{
		FailureThreshold: 5,
		ResetTimeout:     30 * time.Second, // long — we want it STUCK open during phase 2
		InitialTimeout:   5 * time.Second,
	})).Build()

	// PHASE 1 (synchronous): trip the breaker with 5 sequential caller calls.
	// Each call invokes the handler once (CB is in Closed state, passes through)
	// and Observe() increments the failure counter.
	// After the 5th call, the breaker is Open — deterministically.
	for range 5 {
		_, err := act.Do(context.Background(), 1)
		// First 5 calls should NOT be rejected (CB still closed or just opened
		// on the 5th Observe). The error kind is KindUnavailable, not CircuitBreaker.
		if xerr.KindFrom(err) != xerr.KindUnavailable {
			t.Fatalf("phase 1 call returned unexpected kind: %v", err)
		}
	}

	callsAfterPhase1 := calls.Load()
	t.Logf("phase 1 complete: %d handler calls, breaker is Open", callsAfterPhase1)

	// PHASE 2 (concurrent): 50 callers must ALL be rejected by the open CB.
	// The handler must NOT be invoked — the CB fast-fails before reaching it.
	const N = 50
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), 1)
		}(i)
	}
	close(start)
	wg.Wait()

	// Every concurrent caller must have been rejected with KindCircuitBreaker.
	rejected := 0
	for i, e := range errs {
		if e == nil {
			t.Errorf("caller %d: expected error, got nil (CB should be OPEN)", i)
			continue
		}
		if xerr.KindFrom(e) == xerr.KindCircuitBreaker {
			rejected++
		} else {
			t.Errorf("caller %d: expected KindCircuitBreaker, got %v", i, e)
		}
	}

	if rejected != N {
		t.Fatalf("expected all %d callers rejected by open CB, got %d", N, rejected)
	}

	// Handler must NOT have been invoked during phase 2.
	if got := calls.Load(); got != callsAfterPhase1 {
		t.Fatalf("handler was invoked during phase 2 (CB should have fast-failed): before=%d, after=%d",
			callsAfterPhase1, got)
	}
}

// ─── ExclusiveFenced: exactly one winner per acquire cycle ───────────────────
//
// 10 concurrent contenders race for the same lock. The contract is:
// EXACTLY ONE winner per acquire cycle, the rest get ErrLocked immediately.
// Not "at least one" — exactly one.
//
// Deterministic structure (no shared-slice races):
//   - Each goroutine atomically records its outcome (win/reject) BEFORE
//     returning from act.Do — atomic counters, no slice writes.
//   - The winner holds the lock via a release channel; the 9 losers
//     must all return with ErrLocked BEFORE the winner releases.
//   - After release: exactly 1 win, 9 rejects, 1 handler call, lock released.
func TestExclusiveFenced_Concurrent_10Contenders_ExactlyOneWinner(t *testing.T) {
	t.Parallel()

	mu := &singleAcquireMutex{}
	var (
		calls   atomic.Int32
		wins    atomic.Int32
		rejects atomic.Int32
	)

	release := make(chan struct{})
	act := action.New("fenced.concurrent", func(_ context.Context, _ string) (string, error) {
		calls.Add(1)
		<-release // winner holds the lock until test releases
		return "winner", nil
	}).ExclusiveFenced(mu, 1*time.Second, func(_ string) string { return "k" }).Build()

	const N = 10
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range N {
		wg.Go(func() {
			<-start
			_, err := act.Do(context.Background(), "k")
			if err == nil {
				wins.Add(1)
			} else if errors.Is(err, action.ErrLocked) {
				rejects.Add(1)
			}
		})
	}
	close(start)

	// Wait until the winner is inside the handler (lock is held).
	xtest.Eventually(t, 2*time.Second, func() bool { return calls.Load() == 1 })

	// The 9 losers must have returned BEFORE the winner releases — they
	// get ErrLocked immediately on Acquire=false. Poll atomically.
	xtest.Eventually(t, 2*time.Second, func() bool { return rejects.Load() == N-1 })

	// Release the winner — it returns and increments wins.
	close(release)
	wg.Wait()

	// Final accounting: exactly 1 winner, exactly N-1 rejections, 1 handler call.
	if got := wins.Load(); got != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", got)
	}
	if got := rejects.Load(); got != N-1 {
		t.Fatalf("expected exactly %d rejections, got %d", N-1, got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 handler invocation, got %d", got)
	}

	// Verify the mutex was released — next acquire must succeed.
	if _, _, err := mu.Acquire(context.Background(), "verify", 100*time.Millisecond); err != nil {
		t.Fatalf("lock was not released after winner completed: %v", err)
	}
}

// ─── StateMachine: concurrent transitions on separate entities ──────────────

// N goroutines each operate on their OWN entity. The state machine must
// not interfere across entities — every transition should succeed cleanly.
// (Concurrent transitions on a SHARED entity are the application's
// responsibility — the kernel does not serialize entity access.)
func TestStateMachine_Concurrent_TransitionsOnSeparateEntities(t *testing.T) {
	t.Parallel()

	handler := func(_ context.Context, o *Order) (*Order, error) {
		time.Sleep(time.Millisecond) // widen race window
		o.SetState("paid")
		return o, nil
	}

	sm := action.NewStateMachine("order.concurrent", handler).
		Allow("pending", "paid").
		Build()

	const N = 50
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			o := &Order{State: "pending"} // each goroutine gets its own entity
			_, errs[idx] = sm.Do(context.Background(), o)
			if errs[idx] == nil && o.State != "paid" {
				t.Errorf("goroutine %d: state not updated", idx)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// Every transition must succeed because each goroutine has its own entity.
	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, e)
		}
	}
}

// ─── Saga: concurrent sagas on different inputs don't interfere ─────────────

func TestSaga_Concurrent_IsolatedSagas(t *testing.T) {
	t.Parallel()

	saga := action.NewSaga[int, int]("saga.concurrent").
		AddStep("double", func(_ context.Context, n int) (int, error) {
			return n * 2, nil
		}, nil).
		Build()

	const N = 50
	results := make([]int, N)
	errs := make([]error, N)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			res, err := saga.Do(context.Background(), idx+1)
			results[idx] = res.Output
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if errs[i] != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
		}
		if r != (i+1)*2 {
			t.Errorf("goroutine %d: expected %d, got %d", i, (i+1)*2, r)
		}
	}
}

// ─── FanOut: high concurrency preserves input order ─────────────────────────

func TestFanOut_Concurrent_OrderPreservedUnderLoad(t *testing.T) {
	t.Parallel()

	// Variable-latency handler — order of completion is NOT input order.
	act := action.New("fanout.order", func(_ context.Context, n int) (int, error) {
		// Lower n = slower — invert completion order.
		time.Sleep(time.Duration(50-n) * time.Millisecond)
		return n, nil
	}).Build()

	reqs := make([]int, 20)
	for i := range reqs {
		reqs[i] = i + 1
	}

	results := action.FanOut(context.Background(), act, reqs, 4)
	if len(results) != len(reqs) {
		t.Fatalf("expected %d results, got %d", len(reqs), len(results))
	}

	// Order MUST match input despite completion order being inverted.
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error at index %d: %v", i, r.Err)
		}
		if r.Value != reqs[i] {
			t.Fatalf("index %d: expected %d, got %d", i, reqs[i], r.Value)
		}
	}
}

// ─── Race: concurrent winners — exactly one success returns ────────────────

func TestRace_Concurrent_ExactlyOneSuccessReturns(t *testing.T) {
	t.Parallel()

	// All reqs succeed — first one to finish wins.
	act := action.New("race.concurrent", func(ctx context.Context, n int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(n) * time.Millisecond):
			return n, nil
		}
	}).Build()

	reqs := []int{30, 20, 10, 5, 1}
	res, err := action.Race(context.Background(), act, reqs)
	ktest.RequireNoError(t, err)

	// The fastest (1ms) should win.
	if res != 1 {
		t.Fatalf("expected winner=1 (fastest), got %d", res)
	}
}

// ─── xtest.Gate: thundering-herd verification for Cache ─────────────────────

// Smoke test verifying Gate works as documented — used as the canonical
// pattern for any future thundering-herd style test in the kernel.
func TestGate_ThunderingHerd_CanonicalPattern(t *testing.T) {
	t.Parallel()

	gate := xtest.NewGate()
	calls := ktest.NewCounter()
	release := make(chan struct{})

	act := action.New("gate.pattern", func(_ context.Context, _ int) (int, error) {
		calls.Inc()
		<-release
		return 1, nil
	}).Build()

	const N = 20
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			gate.Enter() // synchronize start at the barrier
			_, _ = act.Do(context.Background(), 1)
		})
	}

	// Wait until all goroutines have called Enter() — queued at the gate.
	xtest.Eventually(t, 2*time.Second, func() bool { return int(gate.Active()) == N })

	// Release the gate — all goroutines proceed into act.Do() simultaneously.
	gate.Release()

	// All N must reach the handler.
	xtest.Eventually(t, 2*time.Second, func() bool { return int(calls.Load()) == N })

	close(release)
	wg.Wait()
}
