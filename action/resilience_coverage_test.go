// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// This file closes resilience-pattern coverage gaps identified in the audit:
//   - ConcurrencyLimit: previously untested (happy, error, edge, concurrent).
//   - Coalesce: cancellation isolation (the key behavioral difference vs Dedup).
//   - Saga: optional steps, undo-chain continues on Undo failure, AsAction composition.
//   - StateMachine: no-op transition, missing source state.
//   - AdaptiveLoadShedding: Normal tier, nil stats, MaxCPU=0 edge cases.
//   - Race: empty reqs, all-fail returns last error.
//   - Adaptive (CB): concurrent probes in half-open are rejected.
//   - ExclusiveFenced: concurrent contention — one wins, others get ErrLocked.
//   - FanOut: empty reqs, maxConcurrency=0 (unbounded), maxConcurrency > len(reqs).
//
// All tests use ktest/xtest idioms (Run, Simulate, Counter, Eventually,
// RequireGoroutinesAtMost, RequireZeroAlloc) so the test code itself is
// auditable against the same patterns the kernel promotes.

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

// ─── ConcurrencyLimit ────────────────────────────────────────────────────────

// HAPPY: limit not exceeded — call passes through to handler.
func TestConcurrencyLimit_PassesUnderLimit(t *testing.T) {
	t.Parallel()

	calls := ktest.NewCounter()
	act := action.New("conc.under", func(_ context.Context, _ int) (int, error) {
		calls.Inc()
		return 42, nil
	}).ConcurrencyLimit(10).Build()

	ktest.Run(t, act, context.Background(), 1).
		NoError().
		Equals(42)

	calls.Require(t, 1)
}

// ERROR: limit exceeded — returns ErrConcurrencyLimit, handler NOT invoked.
// Verifies that exactly `limit` goroutines enter the handler and the rest
// are rejected immediately (before the gate is released).
func TestConcurrencyLimit_RejectsOverLimit(t *testing.T) {
	t.Parallel()

	calls := ktest.NewCounter()
	release := make(chan struct{})

	act := action.New("conc.over", func(_ context.Context, _ int) (int, error) {
		calls.Inc()
		<-release // block all in-flight
		return 1, nil
	}).ConcurrencyLimit(3).Build()

	ctx := context.Background()
	var wg sync.WaitGroup
	var rejections, successes atomic.Int32

	// Launch 5 concurrent: first 3 enter the handler, 4th and 5th must be rejected.
	for range 5 {
		wg.Go(func() {
			_, err := act.Do(ctx, 1)
			if err != nil {
				rejections.Add(1)
			} else {
				successes.Add(1)
			}
		})
	}

	// Wait for the 3 in-flight to be inside the handler.
	xtest.Eventually(t, 2*time.Second, func() bool { return calls.Load() == 3 })

	// The 4th and 5th caller should have been rejected immediately.
	// Use atomic counter — no race on shared slice.
	xtest.Eventually(t, 2*time.Second, func() bool { return rejections.Load() == 2 })

	// Release the handler — the 3 in-flight must complete successfully.
	close(release)
	wg.Wait()

	if got := rejections.Load(); got != 2 {
		t.Fatalf("expected 2 rejections, got %d", got)
	}
	if got := successes.Load(); got != 3 {
		t.Fatalf("expected 3 successes, got %d", got)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected exactly 3 handler invocations, got %d", got)
	}
}

// EDGE: limit of 0 rejects EVERY call (count starts at 0, Add(1)=1 > 0).
// This is the "circuit fully open" edge case — useful to know for guardrails.
func TestConcurrencyLimit_ZeroRejectsAll(t *testing.T) {
	t.Parallel()

	calls := ktest.NewCounter()
	act := action.New("conc.zero", func(_ context.Context, _ int) (int, error) {
		calls.Inc()
		return 1, nil
	}).ConcurrencyLimit(0).Build()

	ktest.Simulate(t, act, 1, 50, func(tb testing.TB, _ int, err error) {
		if err == nil {
			tb.Errorf("expected ErrConcurrencyLimit, got nil (limit=0 should reject all)")
		} else if !errors.Is(err, action.ErrConcurrencyLimit) {
			tb.Errorf("expected ErrConcurrencyLimit, got: %v", err)
		}
	})

	// Handler must never have been invoked.
	calls.Require(t, 0)
}

// CONCURRENT: 100 concurrent calls under limit=200 (truly under limit) —
// no errors, no panics, no leaks.
func TestConcurrencyLimit_ConcurrentUnderLimit(t *testing.T) {
	t.Parallel()

	baseline := numGoroutines()
	act := action.New("conc.parallel", func(_ context.Context, n int) (int, error) {
		time.Sleep(time.Millisecond) // tiny delay to force overlap
		return n * 2, nil
	}).ConcurrencyLimit(200).Build() // limit well above caller count

	ktest.Simulate(t, act, 1, 100, func(tb testing.TB, res int, err error) {
		if err != nil {
			tb.Errorf("unexpected error: %v", err)
		}
		if res != 2 {
			tb.Errorf("expected 2, got %d", res)
		}
	})

	xtest.RequireGoroutinesAtMost(t, baseline, 500*time.Millisecond, 2)
}

// ─── Coalesce: cancellation isolation ────────────────────────────────────────

// BEHAVIORAL DIFFERENCE: a canceled WAITER exits with ctx.Err(), but the
// in-flight call keeps running for OTHER waiters (the key behavioral
// difference vs Dedup). The executor (first caller) always runs the
// handler to completion under a detached context.
func TestCoalesce_CanceledWaiterDoesNotKillInFlight(t *testing.T) {
	t.Parallel()

	c := action.NewCoalescer()
	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	var handlerCalls atomic.Int32

	act := action.New("coal.cancel.isolation", func(_ context.Context, _ string) (string, error) {
		handlerCalls.Add(1)
		close(handlerEntered)
		<-handlerRelease
		return "completed", nil
	}).Coalesce(c, func(r string) string { return r }).Build()

	// Caller 1 — EXECUTOR: runs the handler. Uses a never-cancel context
	// because the executor's context is NOT propagated to the handler
	// (CoalesceMiddleware uses context.WithoutCancel internally).
	execDone := make(chan struct {
		res string
		err error
	}, 1)
	go func() {
		res, err := act.Do(context.Background(), "shared-key")
		execDone <- struct {
			res string
			err error
		}{res, err}
	}()

	<-handlerEntered // wait for the executor to be inside the handler

	// Caller 2 — WAITER: cancels mid-flight. Must exit with ctx.Err(),
	// but the in-flight handler must NOT be affected.
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterDone := make(chan struct {
		res string
		err error
	}, 1)
	go func() {
		res, err := act.Do(waiterCtx, "shared-key")
		waiterDone <- struct {
			res string
			err error
		}{res, err}
	}()

	// Give caller 2 a moment to register as a waiter.
	time.Sleep(20 * time.Millisecond)

	// Cancel caller 2 — must NOT propagate to the in-flight handler.
	cancelWaiter()
	r2 := <-waiterDone
	if !errors.Is(r2.err, context.Canceled) {
		t.Fatalf("waiter should receive context.Canceled, got: %v", r2.err)
	}

	// Caller 3 — WAITER: still alive, should receive the result when
	// the handler completes. This proves the in-flight call was NOT
	// terminated by caller 2's cancellation.
	aliveDone := make(chan struct {
		res string
		err error
	}, 1)
	go func() {
		res, err := act.Do(context.Background(), "shared-key")
		aliveDone <- struct {
			res string
			err error
		}{res, err}
	}()

	time.Sleep(20 * time.Millisecond)

	// Release the handler — executor and caller 3 must both get the result.
	close(handlerRelease)

	r1 := <-execDone
	if r1.err != nil {
		t.Fatalf("executor should not have errored, got: %v", r1.err)
	}
	if r1.res != "completed" {
		t.Fatalf("executor expected 'completed', got %q", r1.res)
	}

	r3 := <-aliveDone
	if r3.err != nil {
		t.Fatalf("alive waiter should not have errored, got: %v", r3.err)
	}
	if r3.res != "completed" {
		t.Fatalf("alive waiter expected 'completed', got %q", r3.res)
	}

	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("handler should have been called exactly once, got %d", got)
	}
}

// CROSS-ACTION: a shared *Coalescer correctly namespaces keys per action.
// Two actions using the same key value should NOT collide.
func TestCoalesce_CrossActionNamespaceIsolation(t *testing.T) {
	t.Parallel()

	c := action.NewCoalescer()

	callsA := ktest.NewCounter()
	callsB := ktest.NewCounter()

	gate := make(chan struct{})

	actA := action.New("coalesce.a", func(_ context.Context, _ string) (string, error) {
		callsA.Inc()
		<-gate
		return "from-a", nil
	}).Coalesce(c, func(r string) string { return r }).Build()

	actB := action.New("coalesce.b", func(_ context.Context, _ string) (string, error) {
		callsB.Inc()
		<-gate
		return "from-b", nil
	}).Coalesce(c, func(r string) string { return r }).Build()

	// Fire both with the same key value — both should run because the
	// internal key is namespaced as "actionName:key".
	go func() { _, _ = actA.Do(context.Background(), "same-key") }()
	go func() { _, _ = actB.Do(context.Background(), "same-key") }()

	xtest.Eventually(t, 2*time.Second, func() bool {
		return callsA.Load() == 1 && callsB.Load() == 1
	})

	close(gate)
}

// ERROR: handler error is propagated to ALL waiters.
func TestCoalesce_HandlerErrorReachesAllWaiters(t *testing.T) {
	t.Parallel()

	c := action.NewCoalescer()
	gate := make(chan struct{})

	act := action.New("coal.err.propagate", func(_ context.Context, _ string) (string, error) {
		<-gate
		return "", xerr.Unavailable("handler exploded")
	}).Coalesce(c, func(r string) string { return r }).Build()

	const waiters = 5
	errs := make([]error, waiters)
	var wg sync.WaitGroup
	for i := range waiters {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = act.Do(context.Background(), "err-key")
		}(i)
	}

	close(gate)
	wg.Wait()

	for i, e := range errs {
		if e == nil {
			t.Fatalf("waiter %d: expected error, got nil", i)
		}
		if xerr.KindFrom(e) != xerr.KindUnavailable {
			t.Fatalf("waiter %d: expected KindUnavailable, got %v", i, e)
		}
	}
}

// ─── Saga: optional steps, undo-failure chain, AsAction ──────────────────────

// OPTIONAL STEP: failure of an optional step does NOT trigger rollback.
func TestSaga_OptionalStepFailure_DoesNotRollback(t *testing.T) {
	t.Parallel()

	var undoRan atomic.Bool
	saga := action.NewSaga[string, string]("saga.optional").
		AddStep("mandatory.ok",
			func(_ context.Context, _ string) (string, error) { return "ok1", nil },
			func(_ context.Context, _ string) error { undoRan.Store(true); return nil }).
		AddOptionalStep("optional.fails",
			func(_ context.Context, _ string) (string, error) {
				return "", xerr.Internal("optional failure")
			},
			func(_ context.Context, _ string) error { undoRan.Store(true); return nil }).
		AddStep("mandatory.after",
			func(_ context.Context, _ string) (string, error) { return "ok2", nil },
			func(_ context.Context, _ string) error { undoRan.Store(true); return nil }).
		Build()

	res, err := saga.Do(context.Background(), "req")
	ktest.RequireNoError(t, err)

	if !res.Success {
		t.Fatalf("expected saga success, got: %+v", res)
	}
	if undoRan.Load() {
		t.Fatal("undo must NOT run when an optional step fails")
	}

	// The optional step must be recorded as skipped, not as a hard failure.
	// SagaResult.Steps is now a fixed-size [MaxSagaSteps]StepResult array.
	// Use AllSteps() to get a slice view of populated entries.
	var skippedFound, successCount int
	for _, step := range res.AllSteps() {
		if step.Skipped {
			skippedFound++
		}
		if step.Success {
			successCount++
		}
	}
	if skippedFound != 1 {
		t.Fatalf("expected 1 skipped step, got %d", skippedFound)
	}
	if successCount != 2 {
		t.Fatalf("expected 2 successful steps, got %d", successCount)
	}
}

// UNDO CHAIN: a failing Undo does NOT stop the rollback chain —
// every prior Undo must still run.
func TestSaga_UndoFailureDoesNotStopChain(t *testing.T) {
	t.Parallel()

	var undo1, undo2, undo3 atomic.Bool

	saga := action.NewSaga[string, string]("saga.undofail").
		AddStep("s1",
			func(_ context.Context, _ string) (string, error) { return "ok", nil },
			func(_ context.Context, _ string) error { undo1.Store(true); return nil }).
		AddStep("s2",
			func(_ context.Context, _ string) (string, error) { return "ok", nil },
			func(_ context.Context, _ string) error {
				undo2.Store(true)
				return errors.New("undo 2 failed")
			}).
		AddStep("s3",
			func(_ context.Context, _ string) (string, error) { return "ok", nil },
			func(_ context.Context, _ string) error { undo3.Store(true); return nil }).
		AddStep("s4",
			func(_ context.Context, _ string) (string, error) {
				return "", xerr.Internal("terminal failure")
			},
			nil).
		Build()

	res, err := saga.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected saga to fail")
	}
	if !res.RolledBack {
		t.Fatal("expected RolledBack=true")
	}

	// All three Undos must have run despite undo 2 returning an error.
	if !undo3.Load() || !undo2.Load() || !undo1.Load() {
		t.Fatalf("rollback chain stopped early: undo1=%v undo2=%v undo3=%v",
			undo1.Load(), undo2.Load(), undo3.Load())
	}
}

// ASACTION: a built saga can be lifted into a *BuiltAction and invoked via .Do().
func TestSaga_AsAction_LiftsIntoBuiltAction(t *testing.T) {
	t.Parallel()

	saga := action.NewSaga[int, int]("saga.asaction").
		AddStep("double",
			func(_ context.Context, req int) (int, error) { return req * 2, nil },
			nil).
		Build()

	sagaAct := saga.AsAction()
	meta := sagaAct.Describe()

	if meta.Name != "saga.asaction" {
		t.Fatalf("expected action name 'saga.asaction', got %q", meta.Name)
	}

	res, err := sagaAct.Do(context.Background(), 21)
	ktest.RequireNoError(t, err)

	if !res.Success || res.Output != 42 {
		t.Fatalf("expected success with output 42, got: %+v", res)
	}
}

// ROLLBACK UNDER CANCEL: caller context cancellation does NOT stop
// compensating transactions — they run under context.WithoutCancel.
func TestSaga_RollbackRunsUnderUncanceledContext(t *testing.T) {
	t.Parallel()

	var undoRan atomic.Bool

	saga := action.NewSaga[string, string]("saga.uncancel").
		AddStep("step1",
			func(_ context.Context, _ string) (string, error) { return "ok", nil },
			func(ctx context.Context, _ string) error {
				// If the rollback context were canceled, this would
				// return immediately with ctx.Err().
				if ctx.Err() != nil {
					return ctx.Err()
				}
				undoRan.Store(true)
				return nil
			}).
		AddStep("step2",
			func(_ context.Context, _ string) (string, error) {
				return "", errors.New("terminal failure")
			},
			nil).
		Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel caller context

	res, _ := saga.Do(ctx, "req")
	if !res.RolledBack {
		t.Fatal("expected rollback")
	}
	if !undoRan.Load() {
		t.Fatal("undo must have run despite caller context cancellation")
	}
}

// ─── StateMachine: no-op and missing-source edges ───────────────────────────

// NO-OP TRANSITION: handler doesn't change state → validation skipped, success returned.
func TestStateMachine_NoOpTransition_PassesWithoutValidation(t *testing.T) {
	t.Parallel()

	handler := func(_ context.Context, o *Order) (*Order, error) {
		// Don't change state — common in read-then-conditionally-update patterns.
		return o, nil
	}

	sm := action.NewStateMachine("order.noop", handler).
		Allow("pending", "paid").
		Build()

	o := &Order{State: "pending"}
	_, err := sm.Do(context.Background(), o)
	ktest.RequireNoError(t, err)

	if o.State != "pending" {
		t.Fatalf("state should remain 'pending', got %q", o.State)
	}
}

// MISSING SOURCE: transition from an unregistered source state returns
// Conflict with the "invalid transition from 'X'" message.
func TestStateMachine_MissingSourceState_ReturnsConflict(t *testing.T) {
	t.Parallel()

	handler := func(_ context.Context, o *Order) (*Order, error) {
		o.SetState("paid") // try to transition from "shipped" which isn't registered
		return o, nil
	}

	sm := action.NewStateMachine("order.missing", handler).
		Allow("pending", "paid").
		Build()

	o := &Order{State: "shipped"}
	_, err := sm.Do(context.Background(), o)
	ktest.RequireErrorKind(t, err, xerr.KindConflict)

	if o.State != "shipped" {
		t.Fatalf("state should have been rolled back to 'shipped', got %q", o.State)
	}
}

// MULTI-TARGET: a single source can transition to multiple targets.
// The handler reads the desired target from the entity itself (or computes
// it from request data); the middleware validates after the handler runs.
func TestStateMachine_MultipleTargetsFromSameSource(t *testing.T) {
	t.Parallel()

	// Handler reads a "target" set externally on the entity and applies it.
	// In real code, this would come from request fields.
	handler := func(_ context.Context, o *Order) (*Order, error) {
		// Use the ID field as a stand-in for "desired target".
		if o.ID != "" {
			o.SetState(o.ID)
		}
		return o, nil
	}

	sm := action.NewStateMachine("order.multi", handler).
		Allow("pending", "paid", "canceled", "failed").
		Build()

	for _, target := range []string{"paid", "canceled", "failed"} {
		o := &Order{State: "pending", ID: target}
		_, err := sm.Do(context.Background(), o)
		ktest.RequireNoError(t, err)
		if o.State != target {
			t.Fatalf("expected %q, got %q", target, o.State)
		}
	}

	// Invalid target from the same source — handler tries to set "shipped".
	o := &Order{State: "pending", ID: "shipped"}
	_, err := sm.Do(context.Background(), o)
	ktest.RequireErrorKind(t, err, xerr.KindConflict)

	// State should have been rolled back to "pending".
	if o.State != "pending" {
		t.Fatalf("expected rollback to 'pending', got %q", o.State)
	}
}

// ─── AdaptiveLoadShedding: Normal tier, nil stats, MaxCPU=0 ──────────────────

// NORMAL TIER: sheds at 0.85× threshold (between Low's 0.7× and Critical's 1.0×).
func TestLoadShedding_NormalTier_ShedsAtCorrectThreshold(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		cpu      float64
		priority action.Priority
		shed     bool
	}{
		{"low at 75% CPU", 75.0, action.PriorityLow, true},             // 75 > 70 (0.7 × 100)
		{"normal at 80% CPU", 80.0, action.PriorityNormal, false},      // 80 < 85 (0.85 × 100)
		{"normal at 90% CPU", 90.0, action.PriorityNormal, true},       // 90 > 85
		{"critical at 99% CPU", 99.0, action.PriorityCritical, false},  // 99 < 100
		{"critical at 101% CPU", 101.0, action.PriorityCritical, true}, // 101 > 100
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stats := fakeStats{cpu: tc.cpu, gr: 100}
			mw := action.AdaptiveLoadShedding[string, string](stats, action.LoadShedConfig{
				MaxCPU:        100,
				MaxGoroutines: 1000,
			}, tc.priority)
			next := func(_ context.Context, _ string) (string, error) { return "ok", nil }

			_, err := mw(next)(context.Background(), "req")
			if tc.shed && err == nil {
				t.Fatalf("expected load to be shed, got nil err")
			}
			if !tc.shed && err != nil {
				t.Fatalf("expected load to pass, got: %v", err)
			}
			if tc.shed && xerr.KindFrom(err) != xerr.KindUnavailable {
				t.Fatalf("expected KindUnavailable, got %v", err)
			}
		})
	}
}

// NIL STATS: a nil SystemStats is a passthrough — never sheds.
func TestLoadShedding_NilStats_Passthrough(t *testing.T) {
	t.Parallel()

	mw := action.AdaptiveLoadShedding[string, string](nil, action.LoadShedConfig{
		MaxCPU:        100,
		MaxGoroutines: 1000,
	}, action.PriorityLow)
	next := func(_ context.Context, _ string) (string, error) { return "ok", nil }

	res, err := mw(next)(context.Background(), "req")
	ktest.RequireNoError(t, err)
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}
}

// EDGE: MaxCPU=0 and MaxGoroutines=0 disables CPU and goroutine checks respectively.
func TestLoadShedding_DisabledThresholds(t *testing.T) {
	t.Parallel()

	stats := fakeStats{cpu: 99.0, gr: 99999}
	mw := action.AdaptiveLoadShedding[string, string](stats, action.LoadShedConfig{
		MaxCPU:        0, // disabled
		MaxGoroutines: 0, // disabled
	}, action.PriorityLow)
	next := func(_ context.Context, _ string) (string, error) { return "ok", nil }

	_, err := mw(next)(context.Background(), "req")
	ktest.RequireNoError(t, err)
}

// ─── Race: empty reqs, all-fail ──────────────────────────────────────────────

// EMPTY REQS: returns zero-value Res and nil error.
func TestRace_EmptyReqs_ReturnsZero(t *testing.T) {
	t.Parallel()

	act := action.New("race.empty", func(_ context.Context, _ int) (int, error) {
		return 1, nil
	}).Build()

	res, err := action.Race(context.Background(), act, nil)
	ktest.RequireNoError(t, err)
	if res != 0 {
		t.Fatalf("expected zero value, got %d", res)
	}
}

// ALL FAIL: returns the last error received.
func TestRace_AllFail_ReturnsLastError(t *testing.T) {
	t.Parallel()

	lastErr := xerr.Internal("last")
	act := action.New("race.allfail", func(_ context.Context, n int) (int, error) {
		time.Sleep(time.Duration(n) * time.Millisecond) // ordered failures
		if n == 3 {
			return 0, lastErr
		}
		return 0, xerr.Unavailable("earlier")
	}).Build()

	_, err := action.Race(context.Background(), act, []int{1, 2, 3})
	if !errors.Is(err, lastErr) {
		t.Fatalf("expected last error to be returned, got: %v", err)
	}
}

// ─── Adaptive (Circuit Breaker): concurrent probes rejected ──────────────────

// HALF-OPEN EXCLUSION: when in half-open, only one probe is allowed;
// concurrent callers must be rejected with KindCircuitBreaker.
func TestAdaptive_HalfOpen_ConcurrentProbesRejected(t *testing.T) {
	t.Parallel()

	var mode atomic.Int32 // 0 = fail, 1 = block (probe), 2 = succeed

	probeEntered := make(chan struct{})

	act := action.New("cb.halfopen.concurrent", func(ctx context.Context, _ string) (string, error) {
		switch mode.Load() {
		case 0:
			return "", xerr.Internal("fail")
		case 1:
			close(probeEntered)
			<-ctx.Done()
			return "", ctx.Err()
		default:
			return "ok", nil
		}
	}).Use(action.Adaptive[string, string]("cb.halfopen.concurrent", action.AdaptiveConfig{
		FailureThreshold: 1,
		ResetTimeout:     30 * time.Millisecond,
		InitialTimeout:   30 * time.Millisecond,
	})).Build()

	// Trip the breaker.
	mode.Store(0)
	_, _ = act.Do(context.Background(), "req")

	// Wait for half-open window.
	time.Sleep(50 * time.Millisecond)

	// Fire the probe (mode 1 = blocks on ctx).
	mode.Store(1)
	probeCtx, cancelProbe := context.WithCancel(context.Background())
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		_, _ = act.Do(probeCtx, "req")
	}()

	<-probeEntered

	// Concurrent caller while probe is in-flight: must be rejected.
	_, err := act.Do(context.Background(), "req")
	ktest.RequireErrorKind(t, err, xerr.KindCircuitBreaker)

	// Release the probe — should NOT leave the breaker stuck half-open.
	cancelProbe()
	<-probeDone

	// Wait for the next half-open window.
	time.Sleep(50 * time.Millisecond)

	mode.Store(2)
	res, err := act.Do(context.Background(), "req")
	ktest.RequireNoError(t, err)
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}
}

// ─── ExclusiveFenced: concurrent contention ──────────────────────────────────

// CONTENTION: the first caller acquires the lock and enters the handler;
// the second caller (started deterministically AFTER the first is inside
// the handler) must be rejected immediately with ErrLocked.
func TestExclusiveFenced_ConcurrentContention_OneWinsOneRejected(t *testing.T) {
	t.Parallel()

	mu := &singleAcquireMutex{}

	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})

	act := action.New("fenced.contention", func(_ context.Context, _ string) (string, error) {
		close(handlerEntered)
		<-handlerRelease
		return "winner", nil
	}).ExclusiveFenced(mu, 1*time.Second, func(r string) string { return r }).Build()

	// Start the WINNER first.
	winnerErr := make(chan error, 1)
	go func() {
		_, err := act.Do(context.Background(), "k1")
		winnerErr <- err
	}()

	// Wait for the winner to be inside the handler — at this point the
	// lock is held.
	<-handlerEntered

	// NOW start the LOSER — it must be rejected immediately because the
	// lock is already held.
	loserErr := make(chan error, 1)
	loserResult := make(chan string, 1)
	go func() {
		res, err := act.Do(context.Background(), "k1")
		loserErr <- err
		loserResult <- res
	}()

	select {
	case err := <-loserErr:
		if !errors.Is(err, action.ErrLocked) {
			t.Fatalf("loser should have received ErrLocked, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loser did not return within 2s — likely blocked instead of being rejected")
	}

	if res := <-loserResult; res != "" {
		t.Fatalf("loser result should be empty, got %q", res)
	}

	// Release the handler — winner must complete cleanly.
	close(handlerRelease)
	if err := <-winnerErr; err != nil {
		t.Fatalf("winner should not have errored, got: %v", err)
	}
}

// singleAcquireMutex implements FencedMutex. The first Acquire wins;
// subsequent ones return acquired=false until Release is called.
type singleAcquireMutex struct {
	mu       sync.Mutex
	held     bool
	released bool
	fence    int64
}

func (m *singleAcquireMutex) Acquire(_ context.Context, key string, _ time.Duration) (action.LockLease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held {
		return action.LockLease{}, false, nil
	}
	m.held = true
	m.fence++
	return action.LockLease{Key: key, Owner: "single", Fence: m.fence}, true, nil
}

func (m *singleAcquireMutex) Renew(_ context.Context, lease action.LockLease, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held && lease.Fence == m.fence, nil
}

func (m *singleAcquireMutex) Release(_ context.Context, lease action.LockLease) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.held || lease.Fence != m.fence {
		return false, nil
	}
	m.held = false
	m.released = true
	return true, nil
}

// ─── FanOut: edge cases ──────────────────────────────────────────────────────

// EMPTY REQS: returns nil slice.
func TestFanOut_EmptyReqs_ReturnsNil(t *testing.T) {
	t.Parallel()

	act := action.New("fanout.empty", func(_ context.Context, _ int) (int, error) {
		return 1, nil
	}).Build()

	results := action.FanOut(context.Background(), act, nil, 5)
	if results != nil {
		t.Fatalf("expected nil results for empty input, got len=%d", len(results))
	}
}

// UNBOUNDED: maxConcurrency=0 means unbounded — all reqs run concurrently.
func TestFanOut_UnboundedConcurrency(t *testing.T) {
	t.Parallel()

	calls := ktest.NewCounter()
	act := action.New("fanout.unbounded", func(_ context.Context, _ int) (int, error) {
		calls.Inc()
		time.Sleep(20 * time.Millisecond)
		return 1, nil
	}).Build()

	reqs := make([]int, 20)
	for i := range reqs {
		reqs[i] = i
	}

	start := time.Now()
	results := action.FanOut(context.Background(), act, reqs, 0)
	elapsed := time.Since(start)

	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}
	calls.Require(t, 20)

	// With unbounded concurrency, 20 × 20ms should complete in well under 100ms.
	// (Sequential would take 400ms.)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("unbounded FanOut took too long: %v — likely serialized", elapsed)
	}
}

// MAXCONCURRENCY > LEN(REQS): should be clamped to len(reqs).
func TestFanOut_ConcurrencyLargerThanInput(t *testing.T) {
	t.Parallel()

	act := action.New("fanout.clamp", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Build()

	results := action.FanOut(context.Background(), act, []int{1, 2}, 100)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Value != 2 || results[1].Value != 4 {
		t.Fatalf("expected [2, 4], got [%d, %d]", results[0].Value, results[1].Value)
	}
}

// ─── Backoff: edge cases ─────────────────────────────────────────────────────

// EXPONENTIAL OVERFLOW: extreme attempt values must not panic or overflow.
func TestExponentialBackoff_OverflowSafe(t *testing.T) {
	t.Parallel()

	b := action.ExponentialBackoff(100*time.Millisecond, 5*time.Second)
	for _, attempt := range []int{0, 1, 30, 31, 100, 1000} {
		d := b(attempt)
		if d < 0 {
			t.Fatalf("attempt %d: negative duration %v (overflow?)", attempt, d)
		}
		if d > 5*time.Second {
			t.Fatalf("attempt %d: %v exceeds max 5s", attempt, d)
		}
	}
}

// EXPONENTIAL ATTEMPT=0: returns base (same as attempt=1).
func TestExponentialBackoff_AttemptZero(t *testing.T) {
	t.Parallel()

	b := action.ExponentialBackoff(100*time.Millisecond, 5*time.Second)
	if d := b(0); d != 100*time.Millisecond {
		t.Fatalf("attempt 0: expected %v, got %v", 100*time.Millisecond, d)
	}
}

// LINEAR ATTEMPT=0: returns 0 (no delay).
func TestLinearBackoff_AttemptZero(t *testing.T) {
	t.Parallel()

	b := action.LinearBackoff(100 * time.Millisecond)
	if d := b(0); d != 0 {
		t.Fatalf("attempt 0: expected 0, got %v", d)
	}
}

// CONSTANT ZERO: ConstantBackoff(0) returns 0 — used as the nil-backoff fallback.
func TestConstantBackoff_Zero(t *testing.T) {
	t.Parallel()

	b := action.ConstantBackoff(0)
	for _, attempt := range []int{0, 1, 5, 100} {
		if d := b(attempt); d != 0 {
			t.Fatalf("attempt %d: expected 0, got %v", attempt, d)
		}
	}
}

// JITTER BOUNDS AT EXTREME ATTEMPTS: must always stay within [base, max].
func TestExponentialJitter_ExtremeAttemptsBounded(t *testing.T) {
	t.Parallel()

	j := action.ExponentialJitter(100*time.Millisecond, 2*time.Second)
	for _, attempt := range []int{0, 1, 50, 100, 1000} {
		for range 20 {
			d := j(attempt)
			if d < 100*time.Millisecond {
				t.Fatalf("attempt %d: %v below base", attempt, d)
			}
			if d > 2*time.Second {
				t.Fatalf("attempt %d: %v above max", attempt, d)
			}
		}
	}
}

// ─── SmartResilience: CB tripping after threshold ────────────────────────────

// CB TRIPS: after FailureThreshold=5 consecutive non-permanent failures,
// the breaker opens and fast-fails subsequent calls without invoking the handler.
// Each caller call sees 1 Observe() invocation (after all retries are exhausted).
func TestSmartResilience_CircuitOpensAfterThreshold(t *testing.T) {
	t.Parallel()

	calls := ktest.NewCounter()
	act := action.New("smart.cb.open", func(_ context.Context, _ string) (string, error) {
		calls.Inc()
		return "", xerr.Unavailable("down")
	}).InferredResilient().Build()

	ctx := context.Background()

	// Make 5 caller calls — each produces 1 Observe failure for the CB.
	// After the 5th, the CB trips to OPEN.
	for range 5 {
		_, _ = act.Do(ctx, "req")
	}

	callsAfter5 := calls.Load()

	// 6th caller should be fast-failed by the open CB — no more handler calls.
	_, err := act.Do(ctx, "req")
	ktest.RequireErrorKind(t, err, xerr.KindCircuitBreaker)

	if calls.Load() != callsAfter5 {
		t.Fatalf("handler was invoked after CB opened: before=%d, after=%d",
			callsAfter5, calls.Load())
	}
}

// ─── Cache: error path ──────────────────────────────────────────────────────

// HANDLER ERROR: errors are NOT cached — next call re-executes the handler.
func TestCache_HandlerErrorNotCached(t *testing.T) {
	t.Parallel()

	store := newMockStore()
	calls := ktest.NewCounter()
	failing := atomic.Bool{}
	failing.Store(true)

	act := action.New("cache.err", func(_ context.Context, _ string) (string, error) {
		calls.Inc()
		if failing.Load() {
			return "", xerr.Internal("db error")
		}
		return "ok", nil
	}).Cache(time.Minute, func(r string) string { return r }, store).Build()

	ctx := context.Background()

	// First call: handler errors.
	_, err := act.Do(ctx, "k")
	ktest.RequireErrorKind(t, err, xerr.KindInternal)
	calls.Require(t, 1)

	// Cache must NOT have stored the error.
	_, hit, _ := store.Get(ctx, "k")
	if hit {
		t.Fatal("error response was cached — must not be")
	}

	// Second call: handler recovers — re-executed because no cache entry exists.
	failing.Store(false)
	res, err := act.Do(ctx, "k")
	ktest.RequireNoError(t, err)
	if res != "ok" {
		t.Fatalf("expected 'ok', got %q", res)
	}
	calls.Require(t, 2)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func numGoroutines() int {
	// Small sleep to let starter goroutines settle.
	time.Sleep(10 * time.Millisecond)
	return runtime.NumGoroutine()
}
