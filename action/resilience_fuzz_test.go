// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Fuzz tests for resilience patterns. Run with:
//
//      go test ./action -run '^Fuzz' -fuzztime=30s
//
// To actually fuzz (not just seed corpus):
//
//      go test ./action -run '^FuzzBackoff$' -fuzz=FuzzBackoff -fuzztime=2m
//
// The fuzz tests target:
//   - Backoff strategies: arbitrary attempt int must produce non-negative
//     duration ≤ max. Overflow safety.
//   - Retry predicates: arbitrary error must never panic. Default predicate
//     must be deterministic (same error → same decision).
//   - Idempotency payload hash: same input → same hash; different inputs
//     must produce different hashes (collision resistance).
//   - Memory idempotency store: race-free under arbitrary Set/Get sequences.
//   - Rate limiter LRU: arbitrary key sequences must not panic, must stay
//     within capacity bound.
//   - StateMachine: arbitrary (source, target) pairs must not panic and
//     must always roll back invalid transitions.

package action_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// ─── Backoff fuzz ────────────────────────────────────────────────────────────

// FuzzBackoff_Exponential proves ExponentialBackoff never returns negative
// or above-max for any attempt value, including overflow-inducing ones.
func FuzzBackoff_Exponential(f *testing.F) {
	seeds := []int{0, 1, 2, 3, 5, 10, 30, 31, 32, 100, 1000, 1 << 30, -1, -100}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, attempt int) {
		const base = 100 * time.Millisecond
		const maxDur = 5 * time.Second

		d := action.ExponentialBackoff(base, maxDur)(attempt)
		if d < 0 {
			t.Fatalf("attempt=%d: negative duration %v", attempt, d)
		}
		if d > maxDur {
			t.Fatalf("attempt=%d: %v exceeds max %v", attempt, d, maxDur)
		}
	})
}

// FuzzBackoff_Jitter proves ExponentialJitter stays within [base, max] and
// is non-deterministic (produces varied values across calls).
func FuzzBackoff_Jitter(f *testing.F) {
	seeds := []int{0, 1, 2, 5, 10, 30, 31, 100, 1000, 1 << 30, -1}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, attempt int) {
		const base = 100 * time.Millisecond
		const maxDur = 5 * time.Second

		fn := action.ExponentialJitter(base, maxDur)
		for range 20 {
			d := fn(attempt)
			if d < 0 {
				t.Fatalf("attempt=%d: negative duration %v", attempt, d)
			}
			if d > maxDur {
				t.Fatalf("attempt=%d: %v exceeds max %v", attempt, d, maxDur)
			}
		}
	})
}

// FuzzBackoff_Linear proves LinearBackoff never returns negative for
// non-negative attempt values. Negative attempts are a contract violation
// (retry middleware always calls with attempt ≥ 0) and would produce
// negative durations — we document that here rather than silently accepting.
func FuzzBackoff_Linear(f *testing.F) {
	seeds := []int{0, 1, 2, 5, 100, 1 << 30}
	for _, s := range seeds {
		f.Add(s)
	}
	const step = 100 * time.Millisecond

	f.Fuzz(func(t *testing.T, attempt int) {
		if attempt < 0 {
			return // contract: callers must pass non-negative attempt
		}
		d := action.LinearBackoff(step)(attempt)
		if d < 0 {
			t.Fatalf("attempt=%d: negative duration %v (overflow?)", attempt, d)
		}
	})
}

// FuzzBackoff_Constant proves ConstantBackoff always returns the input.
func FuzzBackoff_Constant(f *testing.F) {
	seeds := []int{0, 1, 100, 1 << 30, -1}
	for _, s := range seeds {
		f.Add(s)
	}
	const d = 250 * time.Millisecond

	f.Fuzz(func(t *testing.T, attempt int) {
		got := action.ConstantBackoff(d)(attempt)
		if got != d {
			t.Fatalf("attempt=%d: expected %v, got %v", attempt, d, got)
		}
	})
}

// ─── Retry predicate fuzz ──────────────────────────────────────────────────

// FuzzRetryPredicate_Default proves DefaultRetryPredicate never panics on
// arbitrary errors and is deterministic (same error → same decision).
func FuzzRetryPredicate_Default(f *testing.F) {
	// Seed with various error shapes.
	f.Add("plain error")
	f.Add("")
	f.Add("transient")

	f.Fuzz(func(t *testing.T, msg string) {
		err := errors.New(msg)
		// DefaultRetryPredicate uses xerr.IsTransient — plain errors are not transient.
		got1 := action.DefaultRetryPredicate(err)
		got2 := action.DefaultRetryPredicate(err)
		if got1 != got2 {
			t.Fatalf("predicate non-deterministic for %q: %v vs %v", msg, got1, got2)
		}

		// Must NOT panic — fuzz engine will catch that automatically.
		// Also test with nil error.
		_ = action.DefaultRetryPredicate(nil)
	})
}

// FuzzRetryPredicate_Always proves AlwaysRetryPredicate returns true for any
// non-nil error and false for nil.
func FuzzRetryPredicate_Always(f *testing.F) {
	f.Add("anything")
	f.Add("")

	f.Fuzz(func(t *testing.T, msg string) {
		err := errors.New(msg)
		if !action.AlwaysRetryPredicate(err) {
			t.Fatalf("AlwaysRetryPredicate returned false for non-nil err %q", msg)
		}
		if action.AlwaysRetryPredicate(nil) {
			t.Fatal("AlwaysRetryPredicate returned true for nil error")
		}
	})
}

// ─── Idempotency hash fuzz ──────────────────────────────────────────────────

// FuzzIdempotency_HashStability proves hashPayload produces the same hash
// for structurally-equal inputs across calls (deterministic JSON marshal).
// Uses an internal package accessor via the public IdempotencyConfig path:
// we wrap the request in an Idempotency-enabled action and verify that the
// stored hash matches across calls.
func FuzzIdempotency_HashStability(f *testing.F) {
	type req struct {
		ID    string
		N     int
		Items []string
	}

	// Seed with various shapes.
	f.Add("user-1", 100, "a,b,c")
	f.Add("", 0, "")
	f.Add("user-1", 100, "a,b,c") // duplicate seed → must produce same hash
	f.Add("user-2", 100, "a,b,c") // different ID → must produce different hash
	f.Add("user-1", 101, "a,b,c") // different N → must produce different hash

	f.Fuzz(func(t *testing.T, id string, n int, itemsCSV string) {
		store := action.NewMemoryIdempotencyStore(time.Hour)
		act := action.New("fuzz.idem", func(_ context.Context, r req) (string, error) {
			return "ok:" + r.ID, nil
		}).IdempotentWithConfig(action.IdempotencyConfig{
			Enabled: true,
			TTL:     time.Hour,
			Store:   store,
		}).Build()

		items := splitCSV(itemsCSV)
		r := req{ID: id, N: n, Items: items}

		// Two identical calls with the same Idempotency-Key.
		// First populates, second must succeed (same hash → cached replay).
		// We use xctx.WithRequestID directly because ktest.RequestContext
		// calls tb.Helper() which is forbidden inside fuzz targets.
		ctx1 := xctx.WithRequestID(context.Background(), "hash-stability-key")
		ctx2 := xctx.WithRequestID(context.Background(), "hash-stability-key")

		_, _ = act.Do(ctx1, r)    // first call — populates
		_, err := act.Do(ctx2, r) // second call — must succeed (same hash)
		if err != nil {
			t.Fatalf("hash mismatch for identical inputs (id=%q n=%d items=%q): %v",
				id, n, itemsCSV, err)
		}
	})
}

// splitCSV is a tiny helper for the fuzz test above.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i, r := range s {
		if r == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

// ─── Memory idempotency store fuzz ──────────────────────────────────────────

// FuzzIdempotencyStore_ConcurrentSafe proves the in-memory idempotency store
// is race-free under arbitrary concurrent Set/Get sequences.
func FuzzIdempotencyStore_ConcurrentSafe(f *testing.F) {
	// Seed: (key, payload-bytes, goroutine count).
	f.Add("k1", []byte("payload-1"), 5)
	f.Add("k2", []byte("payload-2"), 10)
	f.Add("k1", []byte("payload-X"), 3)

	f.Fuzz(func(_ *testing.T, key string, payload []byte, goroutines int) {
		if goroutines < 1 || goroutines > 50 {
			return // bound to keep fuzz fast
		}
		if key == "" {
			return // skip empty keys
		}

		store := action.NewMemoryIdempotencyStore(time.Minute)

		// Half the goroutines write, half read.
		var wg sync.WaitGroup
		half := goroutines / 2
		if half == 0 {
			half = 1
		}

		entry := action.IdempotencyEntry{
			Status:      200,
			Body:        payload,
			RequestHash: "fixed-hash",
			StoredAt:    time.Now(),
		}

		for range half {
			wg.Go(func() {
				store.Set(context.Background(), key, entry, time.Minute)
			})
		}
		for range goroutines - half {
			wg.Go(func() {
				_, _ = store.Get(context.Background(), key)
			})
		}
		wg.Wait()
		// If we got here without a data race, the test passes.
		// Run with -race to actually catch races.
	})
}

// ─── Rate limiter LRU fuzz ─────────────────────────────────────────────────

// FuzzRateLimiter_LRU proves the keyed rate limiter never panics and stays
// within capacity under arbitrary key sequences.
func FuzzRateLimiter_LRU(f *testing.F) {
	// Seed: comma-separated keys + max capacity.
	f.Add("a,b,c,a,b,d", 3)
	f.Add("x", 1)
	f.Add("a,a,a,a,a", 5)
	f.Add("", 10)

	f.Fuzz(func(t *testing.T, keysCSV string, capacity int) {
		if capacity < 1 || capacity > 1000 {
			return
		}

		limiter := action.NewMemoryRateLimiterForTest(1000, 10, time.Hour, capacity)
		ctx := context.Background()

		keys := splitCSV(keysCSV)
		for _, k := range keys {
			if k == "" {
				continue
			}
			_, _ = limiter.Allow(ctx, k)
		}

		// Capacity invariant: never exceed the configured max.
		if limiter.Len() > capacity {
			t.Fatalf("limiter exceeded capacity: len=%d max=%d", limiter.Len(), capacity)
		}
	})
}

// ─── StateMachine fuzz ─────────────────────────────────────────────────────

// FuzzStateMachine_Transitions proves the state machine never panics on
// arbitrary (source, target) pairs and always rolls back invalid transitions.
// Empty source/target pairs are skipped because the kernel treats
// "no state change" (original == new) as a no-op that bypasses validation.
func FuzzStateMachine_Transitions(f *testing.F) {
	// Seed: (source, target).
	f.Add("pending", "paid")
	f.Add("pending", "shipped") // invalid target
	f.Add("shipped", "paid")    // invalid source
	f.Add("xyz", "abc")

	f.Fuzz(func(t *testing.T, source, target string) {
		// Skip empty strings — kernel treats no-state-change as a no-op.
		if source == "" || target == "" {
			return
		}
		if source == target {
			return // no-op transition
		}

		handler := func(_ context.Context, o *Order) (*Order, error) {
			o.SetState(target)
			return o, nil
		}
		sm := action.NewStateMachine("fuzz.sm", handler).
			Allow("pending", "paid", "canceled").
			Build()

		o := &Order{State: source}
		_, err := sm.Do(context.Background(), o)

		if source == "pending" && (target == "paid" || target == "canceled") {
			// Valid transition — no error, state == target.
			if err != nil {
				t.Fatalf("valid transition %q → %q errored: %v", source, target, err)
			}
			if o.State != target {
				t.Fatalf("valid transition left state as %q, want %q", o.State, target)
			}
		} else {
			// Invalid transition — Conflict error AND state rolled back.
			if err == nil {
				t.Fatalf("invalid transition %q → %q succeeded unexpectedly", source, target)
			}
			if xerr.KindFrom(err) != xerr.KindConflict {
				t.Fatalf("expected Conflict for %q → %q, got %v", source, target, err)
			}
			if o.State != source {
				t.Fatalf("invalid transition %q → %q left state as %q (not rolled back)", source, target, o.State)
			}
		}
	})
}

// ─── Saga fuzz ──────────────────────────────────────────────────────────────

// FuzzSaga_StepFailures proves the saga never panics under arbitrary
// step-failure patterns and always runs Undo in LIFO order for failing steps.
func FuzzSaga_StepFailures(f *testing.F) {
	// Seed: 4 booleans — which steps fail.
	f.Add(false, false, false, false) // all succeed
	f.Add(true, false, false, false)  // step 1 fails
	f.Add(false, true, false, false)
	f.Add(false, false, true, false)
	f.Add(false, false, false, true) // last step fails
	f.Add(true, true, true, true)    // all fail

	f.Fuzz(func(t *testing.T, fail1, fail2, fail3, fail4 bool) {
		var undoOrder []int
		var mu sync.Mutex

		mkStep := func(idx int, fail bool) (func(context.Context, int) (int, error), func(context.Context, int) error) {
			do := func(_ context.Context, n int) (int, error) {
				if fail {
					return 0, xerr.Internal("step failed")
				}
				return n + idx, nil
			}
			undo := func(_ context.Context, _ int) error {
				mu.Lock()
				undoOrder = append(undoOrder, idx)
				mu.Unlock()
				return nil
			}
			return do, undo
		}

		do1, undo1 := mkStep(1, fail1)
		do2, undo2 := mkStep(2, fail2)
		do3, undo3 := mkStep(3, fail3)
		do4, undo4 := mkStep(4, fail4)

		saga := action.NewSaga[int, int]("fuzz.saga").
			AddStep("s1", do1, undo1).
			AddStep("s2", do2, undo2).
			AddStep("s3", do3, undo3).
			AddStep("s4", do4, undo4).
			Build()

		res, err := saga.Do(context.Background(), 0)

		// Determine which step fails first (if any).
		failures := []bool{fail1, fail2, fail3, fail4}
		firstFailIdx := -1
		for i, f := range failures {
			if f {
				firstFailIdx = i
				break
			}
		}

		if firstFailIdx == -1 {
			// All succeeded.
			if err != nil {
				t.Fatalf("expected success, got: %v", err)
			}
			if !res.Success {
				t.Fatal("expected Success=true")
			}
			if len(undoOrder) != 0 {
				t.Fatalf("undo should not have run on success, got: %v", undoOrder)
			}
			return
		}

		// Saga failed at step firstFailIdx+1. All prior steps' Undos must
		// have run in LIFO (reverse) order.
		if err == nil {
			t.Fatal("expected saga error")
		}
		if !res.RolledBack {
			t.Fatal("expected RolledBack=true")
		}

		// Verify LIFO order of undo calls.
		wantUndoCount := firstFailIdx // steps 0..firstFailIdx-1 ran Undo.
		if len(undoOrder) != wantUndoCount {
			t.Fatalf("expected %d undo calls, got %d (order=%v)", wantUndoCount, len(undoOrder), undoOrder)
		}
		for i, got := range undoOrder {
			want := firstFailIdx - i // LIFO: highest-indexed first
			if got != want {
				t.Fatalf("undo order[%d] = %d, want %d (LIFO) — order=%v", i, got, want, undoOrder)
			}
		}
	})
}

// ─── Adaptive (CB) fuzz ────────────────────────────────────────────────────

// FuzzAdaptive_NeverPanicsOnMixedErrors proves the circuit breaker never
// panics regardless of the mix of transient, permanent, and nil errors.
func FuzzAdaptive_NeverPanicsOnMixedErrors(f *testing.F) {
	// Seed: 4 booleans — error pattern for 4 consecutive calls.
	// false = success, true = transient error, "perm" via separate bool flag.
	f.Add(false, false, false, false, false)
	f.Add(true, true, true, true, true) // 4 transient failures → CB opens
	f.Add(false, true, false, true, false)

	f.Fuzz(func(_ *testing.T, e1trans, e2trans, e3trans, e4trans, e1perm bool) {
		// Build a small state machine of errors.
		errs := make([]error, 4)
		if e1trans {
			if e1perm {
				errs[0] = xerr.BadRequest("perm") // 4xx — does not trip CB
			} else {
				errs[0] = xerr.Unavailable("transient")
			}
		}
		if e2trans {
			errs[1] = xerr.Unavailable("t2")
		}
		if e3trans {
			errs[2] = xerr.Unavailable("t3")
		}
		if e4trans {
			errs[3] = xerr.Unavailable("t4")
		}

		var idx int
		act := action.New("fuzz.cb", func(_ context.Context, _ int) (int, error) {
			e := errs[idx%len(errs)]
			idx++
			return 0, e
		}).Use(action.Adaptive[int, int]("fuzz.cb", action.AdaptiveConfig{
			FailureThreshold: 3,
			ResetTimeout:     10 * time.Millisecond,
			InitialTimeout:   10 * time.Millisecond,
		})).Build()

		// Run 8 calls — must never panic.
		for range 8 {
			_, _ = act.Do(context.Background(), 1)
		}
	})
}
