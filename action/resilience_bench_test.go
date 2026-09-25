// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// This file locks the allocation and latency budgets of every resilience
// pattern into CI-visible benchmarks. Run them with:
//
//      go test ./action -run '^$' -bench=BenchmarkResilience -benchmem -benchtime=200ms
//
// A regression in allocs/op or ns/op shows up immediately. Use the
// accompanying resilience_zeroalloc_test.go for strict 0-alloc assertions
// in unit-test runs.
//
// Design notes:
//   - All benchmarks are deterministic — no random latencies, no scheduler races.
//   - Each benchmark targets the HOT PATH (success on a warmed-up state).
//   - Where possible the harness uses ktest.BenchAction to mirror the
//     canonical kernel benchmark pattern.
//   - Backoff benchmarks are direct function calls (no action wrapping)
//     because backoff is pure: (attempt int) -> time.Duration.

package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

// ─── Retry ───────────────────────────────────────────────────────────────────

// Hot path: single Do call, no retries needed, succeeds immediately.
func BenchmarkResilience_Retry_Success(b *testing.B) {
	act := action.New("bench.retry.ok", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Retry(3, action.ConstantBackoff(0)).Build()

	ktest.BenchAction(b, act, 42)
}

// Hot path: error path with retries that eventually succeed.
// Backoff is zero so wall-clock is dominated by handler + retry overhead.
func BenchmarkResilience_Retry_RecoversAfter2(b *testing.B) {
	var attempts int
	act := action.New("bench.retry.recover", func(_ context.Context, n int) (int, error) {
		attempts++
		if attempts < 3 {
			return 0, xerr.Unavailable("transient")
		}
		attempts = 0 // reset for next iteration
		return n, nil
	}).Retry(3, action.ConstantBackoff(0)).Build()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := act.Do(ctx, 42); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── Backoff strategies ────────────────────────────────────────────────────

func BenchmarkResilience_Backoff_Constant(b *testing.B) {
	fn := action.ConstantBackoff(time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = fn(5)
	}
}

func BenchmarkResilience_Backoff_Linear(b *testing.B) {
	fn := action.LinearBackoff(time.Millisecond)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = fn(5)
	}
}

func BenchmarkResilience_Backoff_Exponential(b *testing.B) {
	fn := action.ExponentialBackoff(time.Millisecond, time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = fn(5)
	}
}

// ExponentialJitter uses crypto/rand — slightly more expensive per call.
func BenchmarkResilience_Backoff_ExponentialJitter(b *testing.B) {
	fn := action.ExponentialJitter(time.Millisecond, time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = fn(5)
	}
}

// ─── RateLimit ──────────────────────────────────────────────────────────────

// Hot path: under limit, token bucket always allows.
func BenchmarkResilience_RateLimit_Pass(b *testing.B) {
	act := action.New("bench.ratelimit", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).RateLimit(1e9, 1e9).Build() // 1e9 rps, 1e9 burst — effectively unlimited

	ktest.BenchAction(b, act, 42)
}

// ─── ConcurrencyLimit ───────────────────────────────────────────────────────

// Hot path: under limit, atomic counter inc/dec.
func BenchmarkResilience_ConcurrencyLimit_Pass(b *testing.B) {
	act := action.New("bench.conc", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).ConcurrencyLimit(1000).Build() // generous limit to avoid rejections under parallel bench

	ktest.BenchAction(b, act, 42)
}

// ─── Idempotency ───────────────────────────────────────────────────────────

// Hot path: cache hit — the Idempotency-Key is already in the store.
// Uses the SAME payload on every iteration so the request hash matches
// and the stored response is replayed without invoking the handler.
func BenchmarkResilience_Idempotency_Hit(b *testing.B) {
	store := action.NewMemoryIdempotencyStore(time.Hour)
	act := action.New("bench.idem", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Enabled: true,
		TTL:     time.Hour,
		Store:   store,
	}).Build()

	// Prime: store the response under a fixed request ID.
	ctx := ktest.RequestContext(b, "bench-idem-key")
	_, _ = act.Do(ctx, 42)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := act.Do(ctx, 42); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── FanOut ─────────────────────────────────────────────────────────────────

// Hot path: 10 reqs × maxConcurrency=4 — bounded parallel.
func BenchmarkResilience_FanOut(b *testing.B) {
	act := action.New("bench.fanout", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Build()

	reqs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		results := action.FanOut(ctx, act, reqs, 4)
		if len(results) != len(reqs) {
			b.Fatalf("expected %d results, got %d", len(reqs), len(results))
		}
	}
}

// ─── Cache ─────────────────────────────────────────────────────────────────

// Hot path: cache hit on L1 — handler never invoked.
func BenchmarkResilience_Cache_Hit(b *testing.B) {
	store := newMockStore()
	_ = store.Set(context.Background(), "k", "cached", 0)

	act := action.New("bench.cache", func(_ context.Context, _ string) (string, error) {
		return "fresh", nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := act.Do(ctx, "k"); err != nil || res != "cached" {
			b.Fatalf("res=%q err=%v", res, err)
		}
	}
}

// ─── Dedup ─────────────────────────────────────────────────────────────────

// Hot path: every call after the first gets the shared result.
// The first caller populates the result via the gate; subsequent calls
// all hit the dedup path.
func BenchmarkResilience_Dedup_SharedResult(b *testing.B) {
	release := make(chan struct{})
	close(release) // pre-release so handler returns immediately

	act := action.New("bench.dedup", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Dedup(func(_ int) string { return "fixed-key" }).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 42) // prime the first execution

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := act.Do(ctx, 42); err != nil || res != 84 {
			b.Fatalf("res=%d err=%v", res, err)
		}
	}
}

// ─── Coalesce ──────────────────────────────────────────────────────────────

// Hot path: shared coalescer, single in-flight call completes instantly,
// every subsequent call gets the shared result.
func BenchmarkResilience_Coalesce_SharedResult(b *testing.B) {
	c := action.NewCoalescer()
	act := action.New("bench.coalesce", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Coalesce(c, func(_ int) string { return "fixed-key" }).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 42) // prime

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := act.Do(ctx, 42); err != nil || res != 84 {
			b.Fatalf("res=%d err=%v", res, err)
		}
	}
}

// ─── SmartResilience ────────────────────────────────────────────────────────

// Hot path: CB closed, no retries needed — pure middleware overhead.
func BenchmarkResilience_SmartResilience_Success(b *testing.B) {
	act := action.New("bench.smart", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).InferredResilient().Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up the CB state

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := act.Do(ctx, 42); err != nil || res != 84 {
			b.Fatalf("res=%d err=%v", res, err)
		}
	}
}

// ─── ExclusiveFenced ────────────────────────────────────────────────────────

// Hot path: lock acquire + immediate release. Handler completes instantly.
// The fakeMutex has no I/O — pure acquire/release overhead.
func BenchmarkResilience_ExclusiveFenced_AcquireRelease(b *testing.B) {
	mu := &singleAcquireMutex{}
	act := action.New("bench.fenced", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).ExclusiveFenced(mu, 1*time.Second, func(_ int) string { return "k" }).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := act.Do(ctx, 42); err != nil || res != 84 {
			b.Fatalf("res=%d err=%v", res, err)
		}
	}
}

// ─── Saga ──────────────────────────────────────────────────────────────────

// Hot path: 3 steps all succeed — pure saga overhead (StepResult allocation,
// time.Since calls, no rollback).
// Output is the LAST step's result: double(21)=42 → plus1(21)=22 → minus1(21)=20.
// Each step receives the ORIGINAL request, not the previous step's output.
func BenchmarkResilience_Saga_Success(b *testing.B) {
	saga := action.NewSaga[int, int]("bench.saga").
		AddStep("double", func(_ context.Context, n int) (int, error) { return n * 2, nil }, nil).
		AddStep("plus1", func(_ context.Context, n int) (int, error) { return n + 1, nil }, nil).
		AddStep("minus1", func(_ context.Context, n int) (int, error) { return n - 1, nil }, nil).
		Build()

	ctx := context.Background()
	_, _ = saga.Do(ctx, 21) // warm up

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		res, err := saga.Do(ctx, 21)
		if err != nil || !res.Success || res.Output != 20 {
			b.Fatalf("res=%+v err=%v", res, err)
		}
	}
}

// ─── Adaptive (Circuit Breaker) ───────────────────────────────────────────

// Hot path: CB closed, every call passes through with context.WithTimeout.
func BenchmarkResilience_Adaptive_ClosedState(b *testing.B) {
	act := action.New("bench.cb", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Use(action.Adaptive[int, int]("bench.cb", action.AdaptiveConfig{
		FailureThreshold: 5,
		ResetTimeout:     time.Second,
		InitialTimeout:   time.Second,
	})).Build()

	ctx := context.Background()
	_, _ = act.Do(ctx, 1) // warm up

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := act.Do(ctx, 42); err != nil || res != 84 {
			b.Fatalf("res=%d err=%v", res, err)
		}
	}
}

// ─── AdaptiveLoadShedding ──────────────────────────────────────────────────

// Hot path: under all thresholds — pure middleware overhead.
func BenchmarkResilience_AdaptiveLoadShedding_Pass(b *testing.B) {
	stats := fakeStats{cpu: 10.0, gr: 50}
	mw := action.AdaptiveLoadShedding[int, int](stats, action.LoadShedConfig{
		MaxCPU:        100,
		MaxGoroutines: 1000,
	}, action.PriorityNormal)
	wrapped := mw(func(_ context.Context, n int) (int, error) { return n * 2, nil })

	ctx := context.Background()
	_, _ = wrapped(ctx, 1) // warm up

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if res, err := wrapped(ctx, 42); err != nil || res != 84 {
			b.Fatalf("res=%d err=%v", res, err)
		}
	}
}

// ─── Race ──────────────────────────────────────────────────────────────────

// Hot path: N reqs race, fastest (1ms) wins. This is the common case for
// hedged reads — usually the closest replica responds first.
func BenchmarkResilience_Race_Fastest(b *testing.B) {
	act := action.New("bench.race", func(ctx context.Context, n int) (int, error) {
		if n == 1 {
			return n, nil // fastest — immediate
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
			return n, nil
		}
	}).Build()

	reqs := []int{1, 2, 3}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := action.Race(ctx, act, reqs); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── StateMachine ──────────────────────────────────────────────────────────

// Hot path: valid transition. Handler mutates state, middleware validates.
func BenchmarkResilience_StateMachine_ValidTransition(b *testing.B) {
	handler := func(_ context.Context, o *Order) (*Order, error) {
		o.SetState("paid")
		return o, nil
	}

	sm := action.NewStateMachine("bench.sm", handler).
		Allow("pending", "paid").
		Build()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		o := &Order{State: "pending"}
		if _, err := sm.Do(ctx, o); err != nil {
			b.Fatal(err)
		}
	}
}
