// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Contention benchmarks: the same hot paths as bench_test.go and
// resilience_bench_test.go, but executed under b.RunParallel with a
// configurable -cpu sweep. Sequential micro-benchmarks cannot expose lock
// contention, atomic cacheline ping-pong or scheduler effects — these can.
//
// Run them with:
//
//      go test ./action -run '^$' -bench=BenchmarkContention -benchmem -cpu=1,4,8
//
// Interpretation: compare each result against
// BenchmarkContention_Baseline_TypedAction_Parallel (pure dispatch, no
// resilience middleware) at the same -cpu level. The delta is the true
// contended cost of the middleware, not its sequential cost.
//
// Sinks: every result is written to a package-level sink. Without a sink the
// compiler dead-code-eliminates inlinable calls (ConstantBackoff previously
// "measured" 0.27 ns/op — the cost of an empty loop).

package action_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest/ktest"
)

// Benchmark sinks shared by all benchmark files in this package
// (bench_test.go, build_test.go, resilience_bench_test.go).
//
// NOTE on why these are pointers and not plain package variables: the Go 1.26
// compiler performs whole-package dead-store analysis. A store to a package
// variable that is provably never READ anywhere in the package is deleted,
// which resurrects dead-code elimination of the computed value (observed:
// 0.19–0.28 ns/op on otherwise-eliminated benchmark bodies even WITH a plain
// global sink). Storing through a pointer defeats the analysis — the compiler
// cannot rule out that the allocation aliases memory read elsewhere.
var (
	benchSinkIntP      = new(int)
	benchSinkDurationP = new(time.Duration)
	benchSinkActionsP  = new([]action.AnyAction)
	benchSinkActionP   = new(action.AnyAction)
	benchSinkEntryP    = new(action.IdempotencyEntry)
	benchSinkStrP      = new(string)
	benchSinkOkP       = new(bool)
)

func contentionHandler(_ context.Context, n int) (int, error) { return n * 2, nil }

// ─── Baseline ──────────────────────────────────────────────────────────────

// Pure typed dispatch under parallel load — the reference point every other
// contention benchmark is compared against.
func BenchmarkContention_Baseline_TypedAction_Parallel(b *testing.B) {
	act := action.New("bench.baseline.p", contentionHandler).Build()
	ktest.BenchAction(b, act, 42)
}

// ─── RateLimit ─────────────────────────────────────────────────────────────

// Token bucket pass path under parallel load. Effectively unlimited rate, so
// every call is a pass; the measured cost is bucket check + mutex/atomic
// contention on the shared bucket state.
func BenchmarkContention_RateLimit_Parallel(b *testing.B) {
	act := action.New("bench.ratelimit.p", contentionHandler).
		RateLimit(1e15, 1e15).Build()
	ktest.BenchAction(b, act, 42)
}

// ─── ConcurrencyLimit ──────────────────────────────────────────────────────

// Semaphore pass path under parallel load. Limit is generous relative to
// GOMAXPROCS workers, so no rejection happens; the cost is the shared
// in-flight counter.
func BenchmarkContention_ConcurrencyLimit_Parallel(b *testing.B) {
	act := action.New("bench.conc.p", contentionHandler).
		ConcurrencyLimit(1 << 20).Build()
	ktest.BenchAction(b, act, 42)
}

// ─── Circuit breaker (Adaptive) ────────────────────────────────────────────

// Closed-state breaker under parallel load: RWMutex traffic in
// AllowRequest/Observe plus a context.WithTimeout per call. Sequential runs
// showed 413 ns/op; this shows how much of it survives contention.
func BenchmarkContention_CircuitBreaker_Parallel(b *testing.B) {
	act := action.New("bench.cb.p", contentionHandler).
		Use(action.Adaptive[int, int]("bench.cb.p", action.AdaptiveConfig{
			FailureThreshold: 5,
			ResetTimeout:     time.Second,
			InitialTimeout:   time.Second,
		})).Build()
	ktest.BenchAction(b, act, 42)
}

// ─── SmartResilience ───────────────────────────────────────────────────────

// The full composite (retry + jitter backoff + circuit breaker) on the
// success path under parallel load.
func BenchmarkContention_SmartResilience_Parallel(b *testing.B) {
	act := action.New("bench.smart.p", contentionHandler).InferredResilient().Build()
	ktest.BenchAction(b, act, 42)
}

// ─── Idempotency ───────────────────────────────────────────────────────────

// Idempotency HIT through the full middleware under parallel load: every
// worker resolves the same RequestID, so all workers contend on one store key
// and on the SHA-256 payload hash computed per call.
func BenchmarkContention_Idempotency_Hit_Parallel(b *testing.B) {
	store := action.NewMemoryIdempotencyStore(time.Hour)
	act := action.New("bench.idem.p", contentionHandler).
		IdempotentWithConfig(action.IdempotencyConfig{
			Enabled: true,
			TTL:     time.Hour,
			Store:   store,
		}).Build()

	// Prime: store the response under the shared request ID.
	if _, err := act.Do(ktest.RequestContext(b, "bench-idem-key-p"), 42); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := ktest.RequestContext(b, "bench-idem-key-p")
		for pb.Next() {
			res, err := act.Do(ctx, 42)
			*benchSinkIntP = res
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// Store-level Get on MemoryIdempotencyStore under parallel load — isolates
// the store's mutex contention from the middleware around it.
func BenchmarkContention_MemIdemStore_Get_Parallel(b *testing.B) {
	store := action.NewMemoryIdempotencyStore(time.Hour)
	ctx := context.Background()
	store.Set(ctx, "bench.key", action.IdempotencyEntry{Status: 200, Body: []byte("ok")}, time.Hour)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entry, ok := store.Get(ctx, "bench.key")
			*benchSinkEntryP = entry
			*benchSinkOkP = ok
		}
	})
}

// ─── Cache ─────────────────────────────────────────────────────────────────

// L1 cache hit under parallel load. The mock store serializes Gets with a
// mutex, so this measures the contended read path (compare with the
// sequential 61 ns/op BenchmarkResilience_Cache_Hit).
func BenchmarkContention_Cache_Hit_Parallel(b *testing.B) {
	store := newMockStore()
	_ = store.Set(context.Background(), "k", "cached", 0)

	act := action.New("bench.cache.p", func(_ context.Context, r string) (string, error) {
		return r, nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			res, err := act.Do(ctx, "k")
			*benchSinkStrP = res
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// ─── Registry ──────────────────────────────────────────────────────────────

// Control benchmark: read-only map lookup with no lock. Expected to stay flat
// or improve as -cpu grows; if it degrades, the shared map is the problem.
func BenchmarkContention_Registry_Get_Parallel(b *testing.B) {
	act := action.New("bench.registry.p", contentionHandler).Build()
	reg := action.MustNewRegistry(action.Library{Name: "bench", Actions: []action.AnyAction{act}})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a, ok := reg.Get("bench.registry.p")
			*benchSinkActionP = a
			*benchSinkOkP = ok
		}
	})
}

// ─── Pool (round-robin dispatch) ───────────────────────────────────────────

// The sequential BenchmarkPool_Typed_RoundRobin hides the shared atomic
// round-robin counter cost. Under RunParallel every Do hits the same
// counter.Add — this benchmark measures that cacheline contention.
func BenchmarkContention_Pool_RoundRobin_Parallel(b *testing.B) {
	members := make([]*action.BuiltAction[int, int], 10)
	for i := range members {
		members[i] = action.New("pool.member.p", contentionHandler).Build()
	}

	var counter atomic.Uint64
	act := action.New("pool.p", func(ctx context.Context, n int) (int, error) {
		slot := counter.Add(1) - 1
		return members[slot%uint64(len(members))].Do(ctx, n)
	}).Build()

	ktest.BenchAction(b, act, 42)
}
