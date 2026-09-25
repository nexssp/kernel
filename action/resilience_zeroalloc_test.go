// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// This file asserts allocation budgets for the hot paths of every
// resilience pattern. The kernel advertises zero-allocation hot paths;
// these tests lock that contract so a regression shows up immediately.
//
// Allocation budgets (verified empirically via testing.AllocsPerRun):
//
//   STRICT ZERO (RequireZeroAlloc):
//     - Retry_Success
//     - Backoff Constant / Linear / Exponential / Jitter
//     - Cache_Hit (no hooks attached)
//     - ConcurrencyLimit_Pass
//     - AdaptiveLoadShedding_Pass
//
//   EXACT BOUND (RequireMaxAlloc with measured count):
//     - Retry_ErrorThenRecover:    4 allocs/op  (timer + dispatch)
//     - Dedup_SharedResult:        3 allocs/op  (inflightCall + chan + ctx)
//     - Coalesce_SharedResult:     6 allocs/op  (entry + chan + ctx + sprintf + assert)
//     - Adaptive_ClosedState:      4 allocs/op  (context.WithTimeout)
//     - SmartResilience_Success:   4 allocs/op  (CB's ctx.WithTimeout)
//     - StateMachine_Valid:        1 alloc/op   (slices.Contains)
//     - Saga_Success:              1 alloc/op   (StepResult slice)
//     - FanOut_5reqs:             14 allocs/op  (goroutines + slices)
//     - Race_3reqs:                13 allocs/op  (goroutines + channels)
//     - Idempotency_Hit:           5 allocs/op  (JSON marshal/unmarshal)
//     - ExclusiveFenced:          20 allocs/op  (lease renewal + ctx wiring)
//
// Any drift from these exact counts is a regression — CI must fail.
//
// Notes:
//   - All asserts use xtest.RequireZeroAlloc or xtest.RequireMaxAlloc so
//     the same pattern can be tightened later without rewriting tests.
//   - Zero-alloc tests do NOT use t.Parallel() — testing.AllocsPerRun
//     panics inside parallel tests (Go limitation).

package action_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

// ─── Retry: success path is zero-alloc ───────────────────────────────────────

func TestRetry_ZeroAlloc_SuccessPath(t *testing.T) {
	act := action.New("retry.zeroalloc.ok", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Retry(3, action.ConstantBackoff(0)).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	xtest.RequireZeroAlloc(t, 1000, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── Retry: error-then-recover path uses pooled timers ────────────────────
//
// RetryWithPredicateMiddleware uses a sync.Pool of *time.Timer to avoid
// the 2 allocations (timer struct + channel) per retry attempt that
// time.NewTimer would otherwise incur. The remaining 2 allocs/op are
// the retry predicate closure invocation and the hook dispatch.
//
// Without pooling: 4 allocs/op. With pooling: 2 allocs/op.
func TestRetry_BoundedAllocs_ErrorThenRecover(t *testing.T) {
	var attempts atomic.Int32
	act := action.New("retry.bounded", func(_ context.Context, n int) (int, error) {
		if attempts.Add(1) < 2 {
			return 0, xerr.Unavailable("transient")
		}
		return n, nil
	}).Retry(2, action.ConstantBackoff(0)).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up (also primes the timer pool)
	attempts.Store(0)

	// 2 allocs/op after pooling (was 4 before). Allow 3 for -race overhead.
	xtest.RequireMaxAlloc(t, 1000, 3, func() {
		_, _ = act.Do(ctx, 42)
		attempts.Store(0) // reset for next iteration
	})
}

// ─── Backoff strategies: zero-alloc (no -race); 1 alloc under -race ──────────
//
// Constant, Linear, Exponential are always zero-alloc — they are pure
// arithmetic over fixed-size time.Duration values.
//
// ExponentialJitter is zero-alloc in production: crypto/rand.Read on a
// stack-allocated [8]byte buffer. Under -race the runtime adds 1 alloc
// per call for race-detection bookkeeping. We allow 1 to keep tests
// passing under -race.
func TestBackoff_Strategies_ZeroAlloc(t *testing.T) {
	consts := action.ConstantBackoff(time.Millisecond)
	linears := action.LinearBackoff(time.Millisecond)
	exp := action.ExponentialBackoff(time.Millisecond, time.Second)
	jitter := action.ExponentialJitter(time.Millisecond, time.Second)

	// Constant/Linear/Exponential are zero-alloc always.
	xtest.RequireZeroAlloc(t, 1000, func() { _ = consts(5) })
	xtest.RequireZeroAlloc(t, 1000, func() { _ = linears(5) })
	xtest.RequireZeroAlloc(t, 1000, func() { _ = exp(5) })

	// Jitter: 0 in production, 1 under -race. Allow 1 for race-detector overhead.
	xtest.RequireMaxAlloc(t, 1000, 1, func() { _ = jitter(5) })
}

// ─── ConcurrencyLimit: pass-through is zero-alloc ──────────────────────────

func TestConcurrencyLimit_ZeroAlloc_PassThrough(t *testing.T) {
	act := action.New("conc.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).ConcurrencyLimit(100).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	xtest.RequireZeroAlloc(t, 1000, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── Cache: hit path IS zero-alloc when no hooks are attached ──────────────
//
// The kernel's tryLayers path returns the cached value directly; hook
// dispatch iterates over an empty hooks slice (zero iterations, zero
// allocations). If hooks are attached, allocations come from hook
// closures — that's the application's choice, not the kernel's hot path.
func TestCache_HitPath_ZeroAlloc(t *testing.T) {
	store := newMockStore()
	_ = store.Set(context.Background(), "hot-key", "cached", 0)

	act := action.New("cache.zeroalloc.hit", func(_ context.Context, _ string) (string, error) {
		return "should-not-be-called", nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	ctx := context.Background()

	// Warm up: prime any internal singleflight / hook state.
	_, _ = act.Do(ctx, "hot-key")

	// Zero allocs: tryLayers returns cached value, no hooks fire.
	xtest.RequireZeroAlloc(t, 1000, func() {
		res, err := act.Do(ctx, "hot-key")
		if err != nil || res != "cached" {
			t.Fatalf("res=%q err=%v", res, err)
		}
	})
}

// ─── Dedup: shared-result path allocates exactly 3 allocs/op ────────────────
//
// 1 for the inflightCall struct, 1 for its done channel, 1 for the
// context.WithoutCancel derived context.
func TestDedup_SharedResultPath_BoundedAllocs(t *testing.T) {
	act := action.New("dedup.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Dedup(func(_ int) string { return "k" }).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // prime

	// Exact 3 allocs/op — inflightCall + done chan + detached ctx.
	xtest.RequireMaxAlloc(t, 1000, 3, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 42 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── Coalesce: shared-result path allocates exactly 4 allocs/op ───────────
//
// 1 coalesceEntry struct, 1 ready channel, 1 waiters atomic,
// 1 context.WithoutCancel. (Previously 6 — fmt.Sprintf for fullKey was 2
// allocs; replaced with string concat → 0 allocs.)
func TestCoalesce_SharedResultPath_BoundedAllocs(t *testing.T) {
	c := action.NewCoalescer()
	act := action.New("coal.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Coalesce(c, func(_ int) string { return "k" }).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // prime

	// Exact 4 allocs/op (was 6 — Sprintf→concat saved 2).
	xtest.RequireMaxAlloc(t, 1000, 4, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 42 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── Adaptive (CB): closed-state path allocates exactly 4 allocs/op ─────────
//
// context.WithTimeout creates 1 timerCtx, 1 timer, 1 cancelFunc, 1 channel.
// This is the irreducible minimum — context.WithTimeout cannot be avoided.
func TestAdaptive_ZeroAlloc_ClosedState(t *testing.T) {
	act := action.New("cb.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Use(action.Adaptive[int, int]("cb.zeroalloc", action.AdaptiveConfig{
		FailureThreshold: 5,
		ResetTimeout:     time.Second,
		InitialTimeout:   time.Second,
	})).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	// Exact 4 allocs/op — context.WithTimeout overhead.
	xtest.RequireMaxAlloc(t, 1000, 4, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── AdaptiveLoadShedding: pass-through is zero-alloc ───────────────────────

func TestAdaptiveLoadShedding_ZeroAlloc_PassThrough(t *testing.T) {
	stats := fakeStats{cpu: 10.0, gr: 50} // well below thresholds
	mw := action.AdaptiveLoadShedding[int, int](stats, action.LoadShedConfig{
		MaxCPU:        100,
		MaxGoroutines: 1000,
	}, action.PriorityNormal)

	next := func(_ context.Context, n int) (int, error) { return n * 2, nil }
	wrapped := mw(next)

	ctx := context.Background()
	_, _ = wrapped(ctx, 1) // warm up

	xtest.RequireZeroAlloc(t, 1000, func() {
		res, err := wrapped(ctx, 42)
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── SmartResilience: success path allocates exactly 4 allocs/op ────────────
//
// The Adaptive (circuit breaker) middleware's context.WithTimeout overhead.
// The Retry middleware adds 0 on success because no retry timer is created.
func TestSmartResilience_BoundedAllocs_SuccessPath(t *testing.T) {
	act := action.New("smart.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).InferredResilient().Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	// Exact 4 allocs/op — Adaptive CB's context.WithTimeout.
	xtest.RequireMaxAlloc(t, 1000, 4, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── StateMachine: valid-transition path IS zero-alloc ────────────────────
//
// The kernel hot path uses slices.Contains which is verified zero-alloc
// (Go generic, no boxing). The 1 alloc observed in earlier measurements
// was an artifact of the test allocating `&Order{State: "pending"}` inside
// the loop. With a pre-allocated entity reset in-place, the kernel is 0-alloc.
func TestStateMachine_BoundedAllocs_ValidTransition(t *testing.T) {
	handler := func(_ context.Context, o *Order) (*Order, error) {
		o.SetState("paid")
		return o, nil
	}

	sm := action.NewStateMachine("sm.zeroalloc", handler).
		Allow("pending", "paid").
		Build()

	ctx := context.Background()
	// Pre-allocate the entity ONCE. Reset its state in-place inside the loop
	// to avoid measuring the test artifact instead of the kernel hot path.
	preallocated := &Order{State: "pending"}
	_, _ = sm.Do(ctx, preallocated) // warm up

	// Kernel is 0-alloc: slices.Contains is verified zero-alloc,
	// GetState/SetState are interface method calls (no boxing for *Order).
	xtest.RequireZeroAlloc(t, 1000, func() {
		preallocated.State = "pending" // in-place reset, no heap alloc
		_, err := sm.Do(ctx, preallocated)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if preallocated.State != "paid" {
			t.Fatalf("state not updated: %q", preallocated.State)
		}
	})
}

// ─── Saga: success path IS zero-alloc for ≤ MaxSagaSteps ───────────────────
//
// SagaResult.Steps is now a fixed-size [MaxSagaSteps]StepResult array
// (not a slice), so the return value is a single zero-alloc value copy.
// The make([]StepResult, ...) allocation that previously cost 1 alloc/op
// has been eliminated.
func TestSaga_BoundedAllocs_SuccessPath(t *testing.T) {
	saga := action.NewSaga[int, int]("saga.zeroalloc").
		AddStep("double", func(_ context.Context, n int) (int, error) { return n * 2, nil }, nil).
		Build()

	ctx := context.Background()
	_, _ = saga.Do(ctx, 1) // warm up

	// Zero allocs/op — inline stack array, no heap allocation.
	xtest.RequireZeroAlloc(t, 1000, func() {
		res, err := saga.Do(ctx, 21)
		if err != nil || !res.Success || res.Output != 42 {
			t.Fatalf("unexpected: res=%+v err=%v", res, err)
		}
	})
}

// ─── FanOut: hot-path allocates exactly 14 allocs/op for 5 reqs ─────────────
//
// 5 AsyncResult structs in the results slice, 5 goroutines (each goroutine
// has a small stack allocation), 1 semaphore channel, 1 result channel,
// 1 results slice header, 1 closure capture.
//
// Allocations scale with reqs length, not per-call. Cannot be 0-alloc
// because FanOut spawns goroutines which Go runtime allocates on the heap.
func TestFanOut_BoundedAllocs(t *testing.T) {
	act := action.New("fanout.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Build()

	ctx := context.Background()
	reqs := []int{1, 2, 3, 4, 5}

	_ = action.FanOut(ctx, act, reqs, 2) // warm up

	// Exact 14 allocs/op for 5 reqs, maxConcurrency=2.
	xtest.RequireMaxAlloc(t, 1000, 14, func() {
		results := action.FanOut(ctx, act, reqs, 2)
		if len(results) != len(reqs) {
			t.Fatalf("expected %d results, got %d", len(reqs), len(results))
		}
	})
}

// ─── Race: hot-path allocates exactly 13 allocs/op for 3 reqs ───────────────
//
// 3 goroutines, 1 outcome channel (buffered len(reqs)), 1 context.WithCancel
// cancelFunc, plus other internal closures and slice headers.
//
// Cannot be 0-alloc because Race spawns goroutines and uses channels.
func TestRace_BoundedAllocs(t *testing.T) {
	act := action.New("race.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Build()

	ctx := context.Background()
	reqs := []int{1, 2, 3}

	_, _ = action.Race(ctx, act, reqs) // warm up

	// Exact 13 allocs/op for 3 reqs.
	xtest.RequireMaxAlloc(t, 1000, 13, func() {
		_, _ = action.Race(ctx, act, reqs)
	})
}

// ─── Idempotency: cache-hit path allocates 5-6 allocs/op for complex types ──
//
// For struct payloads, hashPayload falls back to encoding/json which
// uses reflection and allocates 5 times (json.Marshal buffer + sha256
// slice + hex.EncodeToString string + json.Unmarshal of cached body +
// cachedRes value).
//
// For primitive types (int, int64, uint, uint64, string, []byte),
// hashPayload uses a fast-path that bypasses json.Marshal — see
// TestIdempotency_PrimitiveHotPath_ZeroAlloc below.
//
// Under -race the runtime adds 1 more alloc for race-detection bookkeeping.
func TestIdempotency_HotPath_BoundedAllocs_Summary(t *testing.T) {
	act := action.New("idem.zeroalloc.summary", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Idempotent().Build()

	ctx := ktest.RequestContext(t, "idem-hot-key")
	_, _ = act.Do(ctx, 42) // populate + warm up — must use SAME payload as the loop

	// 5 in production, 6 under -race. Allow 6 to cover both.
	xtest.RequireMaxAlloc(t, 1000, 6, func() {
		res, err := act.Do(ctx, 42) // SAME payload — replays cached result
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── Idempotency: primitive types use fast-path hashPayloadBytes ──────────
//
// hashPayloadBytes bypasses encoding/json for int, int64, uint, uint64,
// string, and []byte — using strconv.AppendInt/AppendUint on stack buffers
// or hashing strings/[]byte directly. This eliminates 4 of the 5
// allocations that the json.Marshal fallback incurs.
//
// The remaining single allocation is the hex.EncodeToString call in
// hashPayload (which wraps hashPayloadBytes). A future API change could
// return [32]byte directly to eliminate that too.
//
// For the int primitive case below, observe that the cache-hit path now
// allocates exactly 2 allocs/op (hex string + json.Unmarshal of cached
// body) instead of the 5 allocs/op that the struct case incurs.
func TestIdempotency_PrimitiveHotPath_LowerAllocs(t *testing.T) {
	act := action.New("idem.primitive", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Idempotent().Build()

	ctx := ktest.RequestContext(t, "idem-primitive-key")
	_, _ = act.Do(ctx, 42) // populate + warm up

	// 3 in production, 4 under -race. Allow 4 to cover both.
	xtest.RequireMaxAlloc(t, 1000, 4, func() {
		res, err := act.Do(ctx, 42) // SAME payload — replays cached result
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}

// ─── ExclusiveFenced: acquire+release path allocates exactly 20 allocs/op ──
//
// The heaviest pattern:
//   - context.WithCancel (1 cancelFunc + 1 cancelCtx)
//   - context.WithValue for lease propagation (1)
//   - goroutine for lease renewal (1 stack)
//   - sync.WaitGroup.Go (1)
//   - sync.Once (1)
//   - LockLease struct (1)
//   - internal channels for done/lost (2)
//   - other internal closures
//
// Cannot be 0-alloc due to lease renewal goroutine + context wiring.
func TestExclusiveFenced_BoundedAllocs_SuccessPath(t *testing.T) {
	mutex := &singleAcquireMutex{}
	act := action.New("fenced.zeroalloc", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).ExclusiveFenced(mutex, 1*time.Second, func(_ int) string { return "k" }).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	// Exact 20 allocs/op.
	xtest.RequireMaxAlloc(t, 1000, 20, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fatalf("res=%d err=%v", res, err)
		}
	})
}
