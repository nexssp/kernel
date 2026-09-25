// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// This file verifies that resilience-pattern hooks fire with the correct
// arguments under concurrent load. Hooks are the kernel's observability
// contract: if a hook fires with wrong args (or doesn't fire at all),
// metrics, traces, and audit logs are silently corrupted — a critical
// bug for production.
//
// All tests use ktest.Recorder — the canonical hook recorder. Recorder is
// goroutine-safe via sync.RWMutex, so the same instance can be attached
// to multiple concurrent callers without races.
//
// What's verified:
//   - OnRetry fires with correct (req, attempt, err) per attempt.
//   - OnCacheHit fires with cached response, handler NOT invoked.
//   - OnCacheMiss fires on miss, before handler runs.
//   - OnCoalesced fires for waiters, NOT for the executor.
//   - OnDeduplicated fires for waiters, NOT for the executor.
//   - All hooks are race-free under 50+ concurrent callers.
//   - Hook counts match caller counts exactly.

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

// ─── OnRetry: per-attempt arguments ──────────────────────────────────────

// Verify OnRetry fires exactly N times for N retries, each with the correct
// attempt counter (1-indexed) and the error that caused the retry.
func TestHook_OnRetry_Arguments(t *testing.T) {
	t.Parallel()

	rec := &ktest.Recorder[int, int]{}
	var attempts atomic.Int32

	act := action.New("hook.retry", func(_ context.Context, _ int) (int, error) {
		a := attempts.Add(1)
		if a < 3 {
			return 0, xerr.Unavailable("transient")
		}
		return 42, nil
	}).
		HookRetry(func(ctx context.Context, _ int, attempt int, err error, _ *action.Meta) {
			rec.OnRetry(ctx, 0, attempt, err)
		}).
		Retry(3, action.ConstantBackoff(0)).
		Build()

	res, err := act.Do(context.Background(), 1)
	ktest.RequireNoError(t, err)
	if res != 42 {
		t.Fatalf("expected 42, got %d", res)
	}

	if got := len(rec.Retries); got != 2 {
		t.Fatalf("expected 2 OnRetry events (after attempt 1 and 2), got %d", got)
	}

	// Verify attempt indices are 1 and 2 (1-indexed, fire BEFORE the retry sleep).
	if rec.Retries[0].Attempt != 1 {
		t.Fatalf("first retry: attempt = %d, want 1", rec.Retries[0].Attempt)
	}
	if rec.Retries[1].Attempt != 2 {
		t.Fatalf("second retry: attempt = %d, want 2", rec.Retries[1].Attempt)
	}

	// Verify the error is the one that triggered the retry.
	for i, ev := range rec.Retries {
		if ev.Err == nil {
			t.Fatalf("retry event %d has nil err", i)
		}
		if xerr.KindFrom(ev.Err) != xerr.KindUnavailable {
			t.Fatalf("retry event %d err kind = %v, want KindUnavailable", i, xerr.KindFrom(ev.Err))
		}
	}
}

// ─── OnCacheHit: handler NOT invoked, cached response passed ──────────────

func TestHook_OnCacheHit_CachedResponsePassed(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	_ = store.Set(context.Background(), "k1", "cached-value", 0)

	var handlerCalled atomic.Bool
	rec := &ktest.Recorder[string, string]{}

	act := action.New("hook.cachehit", func(_ context.Context, _ string) (string, error) {
		handlerCalled.Store(true)
		return "fresh", nil
	}).
		Cache(time.Minute, func(r string) string { return r }, store).
		HookCacheHit(func(ctx context.Context, req string, res string, _ *action.Meta) {
			rec.OnCacheHit(ctx, req, res)
		}).
		Build()

	res, err := act.Do(context.Background(), "k1")
	ktest.RequireNoError(t, err)

	if res != "cached-value" {
		t.Fatalf("expected cached value, got %q", res)
	}
	if handlerCalled.Load() {
		t.Fatal("handler was invoked on cache hit — must not be")
	}
	if rec.CacheHits != 1 {
		t.Fatalf("expected 1 cache hit event, got %d", rec.CacheHits)
	}
}

// ─── OnCacheMiss: fires before handler runs ──────────────────────────────

func TestHook_OnCacheMiss_FiresBeforeHandler(t *testing.T) {
	t.Parallel()

	store := newMockStore() // empty store — guaranteed miss

	var missFiredBeforeHandler atomic.Bool
	var handlerRan atomic.Bool

	act := action.New("hook.cachemiss", func(_ context.Context, _ string) (string, error) {
		handlerRan.Store(true)
		return "computed", nil
	}).
		Cache(time.Minute, func(r string) string { return r }, store).
		HookCacheMiss(func(_ context.Context, _ string, _ *action.Meta) {
			// OnCacheMiss must fire BEFORE the handler runs.
			if handlerRan.Load() {
				t.Errorf("OnCacheMiss fired after handler — must fire before")
			}
			missFiredBeforeHandler.Store(true)
		}).
		Build()

	_, err := act.Do(context.Background(), "miss-key")
	ktest.RequireNoError(t, err)

	if !missFiredBeforeHandler.Load() {
		t.Fatal("OnCacheMiss did not fire")
	}
	if !handlerRan.Load() {
		t.Fatal("handler did not run on cache miss")
	}
}

// ─── OnDeduplicated: fires for waiters, NOT for executor ─────────────────
//
// Verifies that OnDeduplicated fires exactly N-1 times (once per waiter)
// and that the handler is invoked exactly once (the executor).
//
// Synchronization insight: OnDeduplicated fires AFTER c.Do returns (i.e.
// AFTER release). So we can't check dedupEvents before release. But once
// the executor is inside the handler (blocked on <-release), the entry
// exists in inflightMap and ALL latecomers deterministically become
// waiters — they cannot become new executors. So execCount == 1 is a
// sufficient precondition to safely release.
func TestHook_OnDeduplicated_FiresForWaitersOnly(t *testing.T) {
	t.Parallel()

	var (
		dedupEvents atomic.Int32
		execCount   atomic.Int32
	)
	release := make(chan struct{})

	act := action.New("hook.dedup", func(_ context.Context, _ string) (string, error) {
		execCount.Add(1)
		<-release
		return "shared", nil
	}).
		Dedup(func(r string) string { return r }).
		HookDeduplicatedEvent(func() {
			dedupEvents.Add(1)
		}).
		Build()

	const N = 20
	var wg sync.WaitGroup
	errs := make([]error, N)
	start := make(chan struct{}) // start barrier — all callers arrive at Dedup simultaneously
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), "shared-key")
		}(i)
	}
	close(start)

	// Wait until exactly one caller has become the executor. Once the executor
	// is inside the handler (blocked on <-release), the inflightMap entry
	// exists, so all N-1 other callers deterministically become waiters.
	// We can't check dedupEvents here — it only fires AFTER release.
	xtest.Eventually(t, 5*time.Second, func() bool {
		return execCount.Load() == 1
	})

	// Brief settle: allow any in-flight keyFn calls to reach c.Do.
	// This is necessary because keyFn runs BEFORE c.Do, so a caller can
	// be between keyFn and c.Do when execCount becomes 1.
	time.Sleep(20 * time.Millisecond)

	close(release)
	wg.Wait()

	// Every caller must succeed — verify NO error was returned.
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	// Exactly 1 handler invocation — no latecomer became a new executor.
	if got := execCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 handler invocation, got %d (latecomer became new executor)", got)
	}

	// Exactly N-1 dedup events (one per waiter; executor doesn't fire one).
	if got := dedupEvents.Load(); got != int32(N-1) {
		t.Fatalf("expected %d OnDeduplicated events, got %d", N-1, got)
	}
}

// ─── OnCoalesced: fires for waiters, NOT for executor ────────────────────
//
// Uses Coalescer.Waiters() for deterministic synchronization — eliminates
// the time.Sleep race where latecomers could become new executors.
func TestHook_OnCoalesced_FiresForWaitersOnly(t *testing.T) {
	t.Parallel()

	c := action.NewCoalescer()
	var (
		coalescedEvents atomic.Int32
		execCount       atomic.Int32
	)
	release := make(chan struct{})

	act := action.New("hook.coalesce", func(_ context.Context, _ string) (string, error) {
		execCount.Add(1)
		<-release
		return "shared", nil
	}).
		Coalesce(c, func(r string) string { return r }).
		HookCoalescedEvent(func() {
			coalescedEvents.Add(1)
		}).
		Build()

	// The coalescer namespaces keys as "actionName:key".
	const fullKey = "hook.coalesce:shared-key"

	const N = 20
	var wg sync.WaitGroup
	errs := make([]error, N)
	start := make(chan struct{}) // start barrier
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), "shared-key")
		}(i)
	}
	close(start)

	// Deterministic synchronization via the coalescer's internal state:
	//   - Exactly 1 caller becomes the executor (execCount == 1).
	//   - All N-1 other callers have registered as waiters (Waiters == N-1).
	xtest.Eventually(t, 5*time.Second, func() bool {
		return execCount.Load() == 1 && c.Waiters(fullKey) == N-1
	})

	close(release)
	wg.Wait()

	// Every caller must succeed.
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	// Exactly N-1 coalesced events (one per waiter).
	if got := coalescedEvents.Load(); got != int32(N-1) {
		t.Fatalf("expected %d OnCoalesced events, got %d", N-1, got)
	}
}

// ─── OnRetry: counts under concurrent callers ────────────────────────────

// 50 concurrent callers each retry 2 times (3 attempts total). Total OnRetry
// events must equal 50 × 2 = 100. Verifies hook counting is race-free and
// accurate under load.
func TestHook_OnRetry_Concurrent_AccurateCount(t *testing.T) {
	t.Parallel()

	var retryEvents atomic.Int32

	act := action.New("hook.retry.concurrent", func(_ context.Context, _ int) (int, error) {
		// Always succeed on first try — no retries actually happen.
		return 1, nil
	}).
		HookRetryEvent(func(_ int, _ error) {
			retryEvents.Add(1)
		}).
		Retry(3, action.ConstantBackoff(0)).
		Build()

	const N = 50
	ktest.Simulate(t, act, 1, N, func(tb testing.TB, _ int, err error) {
		if err != nil {
			tb.Errorf("unexpected error: %v", err)
		}
	})

	// No retries should have fired — every caller succeeded on first try.
	if got := retryEvents.Load(); got != 0 {
		t.Fatalf("expected 0 OnRetry events (all succeeded first try), got %d", got)
	}
}

// ─── All hooks fire under heavy concurrent load ──────────────────────────

// Compose Cache + Retry + hooks. 50 concurrent callers with a SLOW handler
// (10ms) so all callers arrive at tryLayers before the executor finishes.
// This guarantees a stampede pattern: all 50 miss on round 1, all 50 hit
// on round 2.
//
// Verify:
//   - Round 1: 0 cache hits, ≤N misses (singleflight collapses), 0 retries
//   - Round 2: N cache hits, 0 misses, 0 retries
//   - No data races (run with -race)
func TestHook_AllHooks_Concurrent_Load(t *testing.T) {
	t.Parallel()

	store := newIntMockStore()
	var (
		cacheHits   atomic.Int32
		cacheMisses atomic.Int32
		retries     atomic.Int32
	)

	act := action.New("hook.all", func(_ context.Context, n int) (int, error) {
		time.Sleep(10 * time.Millisecond) // slow handler → stampede pattern
		return n * 2, nil
	}).
		Cache(time.Minute, func(_ int) string { return "fixed" }, store).
		HookCacheHit(func(context.Context, int, int, *action.Meta) { cacheHits.Add(1) }).
		HookCacheMiss(func(context.Context, int, *action.Meta) { cacheMisses.Add(1) }).
		HookRetry(func(context.Context, int, int, error, *action.Meta) { retries.Add(1) }).
		Retry(3, action.ConstantBackoff(0)).
		Build()

	const N = 50
	ktest.Simulate(t, act, 1, N, func(tb testing.TB, _ int, err error) {
		if err != nil {
			tb.Errorf("first call: unexpected error: %v", err)
		}
	})

	// Round 1: all callers arrive before cache is populated.
	// Singleflight collapses them to 1 handler call.
	// The singleflight LEADER fires OnCacheMiss (1 event).
	// The 49 followers wait via singleflight — they fire NEITHER OnCacheHit
	// NOR OnCacheMiss (they get the result via singleflight, not tryLayers).
	if got := cacheHits.Load(); got != 0 {
		t.Fatalf("round 1: expected 0 cache hits, got %d", got)
	}
	if got := cacheMisses.Load(); got != 1 {
		t.Fatalf("round 1: expected 1 cache miss (singleflight leader), got %d", got)
	}
	if got := retries.Load(); got != 0 {
		t.Fatalf("round 1: expected 0 retry events, got %d", got)
	}

	// Reset counters.
	cacheHits.Store(0)
	cacheMisses.Store(0)
	retries.Store(0)

	// Round 2 — cache is populated, all N should hit.
	ktest.Simulate(t, act, 1, N, func(tb testing.TB, _ int, err error) {
		if err != nil {
			tb.Errorf("second call: unexpected error: %v", err)
		}
	})

	if got := cacheHits.Load(); got != int32(N) {
		t.Fatalf("round 2: expected %d cache hits, got %d", N, got)
	}
	if got := cacheMisses.Load(); got != 0 {
		t.Fatalf("round 2: expected 0 cache misses, got %d", got)
	}
	if got := retries.Load(); got != 0 {
		t.Fatalf("round 2: expected 0 retry events, got %d", got)
	}
}

// ─── OnRetry: error type preserved across retry boundary ─────────────────

// Verify the error passed to OnRetry is the SAME error the handler returned,
// preserving its xerr.Kind and wrapped context.
func TestHook_OnRetry_ErrorTypePreserved(t *testing.T) {
	t.Parallel()

	var capturedErr error

	wrappedErr := xerr.Unavailable("upstream down")
	act := action.New("hook.retry.errtype", func(_ context.Context, _ int) (int, error) {
		return 0, wrappedErr
	}).
		HookRetry(func(_ context.Context, _ int, _ int, err error, _ *action.Meta) {
			capturedErr = err
		}).
		Retry(3, action.ConstantBackoff(0)).
		Build()

	_, err := act.Do(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}

	// capturedErr must be the same wrappedErr the handler returned.
	if !errors.Is(capturedErr, wrappedErr) {
		t.Fatalf("OnRetry received different error: got %v, want %v", capturedErr, wrappedErr)
	}
	// Kind must be preserved.
	if xerr.KindFrom(capturedErr) != xerr.KindUnavailable {
		t.Fatalf("OnRetry error kind = %v, want KindUnavailable", xerr.KindFrom(capturedErr))
	}
}

// ─── Recorder under concurrent load ──────────────────────────────────────

// Smoke test for ktest.Recorder itself — verify it's race-free under
// concurrent OnRetry / OnCacheHit / OnCoalesced / OnDeduplicated calls.
// This is the foundation the tests above rely on.
func TestRecorder_Concurrent_AllHookTypes(t *testing.T) {
	t.Parallel()

	rec := &ktest.Recorder[int, int]{}
	const N = 200

	var wg sync.WaitGroup
	for range N {
		wg.Add(4)
		// Mix all four hook types concurrently.
		go func() {
			defer wg.Done()
			rec.OnRetry(context.Background(), 0, 1, nil)
		}()
		go func() {
			defer wg.Done()
			rec.OnCacheHit(context.Background(), 0, 0)
		}()
		go func() {
			defer wg.Done()
			rec.OnCacheMiss(context.Background(), 0)
		}()
		go func() {
			defer wg.Done()
			rec.OnCoalesced(context.Background(), 0)
		}()
	}
	wg.Wait()

	// Each hook type must have fired exactly N times.
	if len(rec.Retries) != N {
		t.Fatalf("Retries = %d, want %d", len(rec.Retries), N)
	}
	if rec.CacheHits != N {
		t.Fatalf("CacheHits = %d, want %d", rec.CacheHits, N)
	}
	if rec.CacheMisses != N {
		t.Fatalf("CacheMisses = %d, want %d", rec.CacheMisses, N)
	}
	if rec.Coalesced != N {
		t.Fatalf("Coalesced = %d, want %d", rec.Coalesced, N)
	}
}

// ─── OnDeduplicated + OnCacheHit composition ────────────────────────────

// When Dedup is OUTSIDE Cache (added LAST → outermost in the middleware chain),
// concurrent callers collapse via Dedup: 1 executor goes to Cache (miss),
// N-1 waiters receive the result via Dedup and fire OnDeduplicated.
// OnCacheHit is NOT fired for waiters (they never entered Cache).
//
// Middleware order reminder: slices.Backward(b.middlewares) iterates from
// last-added to first-added, so the LAST middleware in the builder chain
// becomes the OUTERMOST at runtime.
//
//	.Dedup(...).Cache(...)  →  middlewares=[Dedup, Cache]  →  exec=Dedup(Cache(handler))
//	.Cache(...).Dedup(...)  →  middlewares=[Cache, Dedup]  →  exec=Cache(Dedup(handler))
//
// For Dedup to see all N callers (and fire N-1 events), Dedup MUST be outermost.
func TestHook_DedupPlusCache_WaitersFireDedupNotCacheHit(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	var (
		dedupEvents     atomic.Int32
		cacheHitEvents  atomic.Int32
		cacheMissEvents atomic.Int32
		release         = make(chan struct{})
	)

	act := action.New("hook.dedup+cache", func(_ context.Context, _ string) (string, error) {
		<-release
		return "computed", nil
	}).
		// ORDER MATTERS: Dedup last → Dedup is outermost.
		// Cache first → Cache is inside Dedup.
		Dedup(func(r string) string { return r }).                     // outermost
		Cache(time.Minute, func(r string) string { return r }, store). // inside Dedup
		HookDeduplicatedEvent(func() { dedupEvents.Add(1) }).
		HookCacheHit(func(context.Context, string, string, *action.Meta) { cacheHitEvents.Add(1) }).
		HookCacheMiss(func(context.Context, string, *action.Meta) { cacheMissEvents.Add(1) }).
		Build()

	const N = 20
	var wg sync.WaitGroup
	errs := make([]error, N)
	// Use a start barrier so all callers arrive at Dedup simultaneously.
	start := make(chan struct{})
	for i := range N {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = act.Do(context.Background(), "shared-key")
		}(i)
	}
	close(start)

	// Give all goroutines time to register with Dedup.
	time.Sleep(50 * time.Millisecond)

	close(release)
	wg.Wait()

	// Every caller must succeed.
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: unexpected error: %v", i, err)
		}
	}

	// First caller (executor): cache miss + handler runs + populates cache.
	// Other N-1 callers (waiters): dedup (share the in-flight result).
	if got := cacheMissEvents.Load(); got != 1 {
		t.Fatalf("expected exactly 1 cache miss, got %d", got)
	}
	if got := cacheHitEvents.Load(); got != 0 {
		t.Fatalf("expected 0 cache hits (waiters bypassed Cache via Dedup), got %d", got)
	}
	if got := dedupEvents.Load(); got != int32(N-1) {
		t.Fatalf("expected %d dedup events, got %d", N-1, got)
	}
}

// ─── Helper: int-typed mock CacheLayer ────────────────────────────────────

// intMockStore is a CacheLayer[int] for the hooks tests that need int responses.
type intMockStore struct {
	mu    sync.Mutex
	store map[string]int
}

func newIntMockStore() *intMockStore {
	return &intMockStore{store: make(map[string]int)}
}

func (m *intMockStore) Get(_ context.Context, key string) (val int, hit bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store[key]
	return v, ok, nil
}

func (m *intMockStore) Set(_ context.Context, key string, val int, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = val
	return nil
}
