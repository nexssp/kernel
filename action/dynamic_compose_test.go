// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestPipeAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	t.Run("success forwards data", func(t *testing.T) {
		left := ktest.Fake("left", "mid")
		right := action.New("right", func(_ context.Context, req any) (any, error) {
			s, ok := req.(string)
			if !ok {
				return nil, errors.New("expected string")
			}
			return s + "_done", nil
		}).Build()

		pipe := action.PipeAny("pipe", left, right).Build()
		ktest.Run(t, pipe, ctx, any("start")).NoError().Equals("mid_done")
	})

	t.Run("left error halts execution", func(t *testing.T) {
		left := ktest.FakeErr("left", xerr.NotFound("missing left"))
		right := ktest.Fake("right", "done")

		pipe := action.PipeAny("pipe", left, right).Build()
		ktest.Run(t, pipe, ctx, any("start")).ErrorKind(xerr.KindNotFound)
	})
}

func TestParallelAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	t.Run("scatter gather success", func(t *testing.T) {
		a := action.New("a", func(_ context.Context, req any) (any, error) {
			s, ok := req.(string)
			if !ok {
				return nil, fmt.Errorf("expected string, got %T", req)
			}
			return s + "_A", nil
		}).Build()
		b := action.New("b", func(_ context.Context, req any) (any, error) {
			s, ok := req.(string)
			if !ok {
				return nil, fmt.Errorf("expected string, got %T", req)
			}
			return s + "_B", nil
		}).Build()

		par := action.ParallelAny("par", a, b).Build()
		res := ktest.Run(t, par, ctx, any("start")).NoError().Value()
		ktest.RequireEqual(t, res["a"], "start_A")
		ktest.RequireEqual(t, res["b"], "start_B")
	})

	t.Run("isolates panics as errors", func(t *testing.T) {
		a := ktest.Fake("a", "ok")
		b := action.New("b", func(_ context.Context, _ any) (any, error) {
			panic("unexpected boom")
		}).Build()

		par := action.ParallelAny("par", a, b).Build()
		ktest.Run(t, par, ctx, any(nil)).ErrorContains("panic recovered")
	})
}

func TestFirstSuccessAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	t.Run("recovers on first success", func(t *testing.T) {
		err1 := ktest.FakeErr("err1", xerr.Timeout("t1"))
		err2 := ktest.FakeErr("err2", xerr.Conflict("c1"))
		ok := ktest.Fake("ok", "saved")

		fs := action.FirstSuccessAny("fs", err1, err2, ok).Build()
		ktest.Run(t, fs, ctx, any(nil)).NoError().Equals("saved")
	})

	t.Run("fails if all fail", func(t *testing.T) {
		err1 := ktest.FakeErr("err1", xerr.Timeout("t1"))
		err2 := ktest.FakeErr("err2", xerr.Conflict("c1"))

		fs := action.FirstSuccessAny("fs", err1, err2).Build()
		ktest.Run(t, fs, ctx, any(nil)).ErrorContains("all failed")
	})
}

func TestRoundRobinAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)
	a := ktest.Fake("a", "A")
	b := ktest.Fake("b", "B")

	rr := action.RoundRobinAny("rr", a, b).Build()

	ktest.Run(t, rr, ctx, any(nil)).NoError().Equals("A")
	ktest.Run(t, rr, ctx, any(nil)).NoError().Equals("B")
	ktest.Run(t, rr, ctx, any(nil)).NoError().Equals("A")
}

func TestLoopAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	t.Run("exits on condition", func(t *testing.T) {
		body := action.New("inc", func(_ context.Context, req any) (any, error) {
			n, ok := req.(int)
			if !ok {
				return nil, fmt.Errorf("expected int, got %T", req)
			}
			return n + 1, nil
		}).Build()

		loop := action.LoopAny("loop", body, func(v any) bool {
			n, ok := v.(int)
			return ok && n >= 3
		}, 5).Build()
		ktest.Run(t, loop, ctx, any(0)).NoError().Equals(3)
	})

	t.Run("fails on max turns", func(t *testing.T) {
		body := action.New("inc", func(_ context.Context, req any) (any, error) {
			n, ok := req.(int)
			if !ok {
				return nil, fmt.Errorf("expected int, got %T", req)
			}
			return n + 1, nil
		}).Build()

		loop := action.LoopAny("loop", body, func(v any) bool {
			n, ok := v.(int)
			return ok && n >= 10
		}, 2).Build()
		ktest.Run(t, loop, ctx, any(0)).ErrorKind(xerr.KindTimeout).ErrorContains("exceeded")
	})
}

func TestRaceAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)
	latch := xtest.NewLatch()

	slow := action.New("slow", func(c context.Context, _ any) (any, error) {
		select {
		case <-c.Done():
			return nil, c.Err()
		case <-latch.Done():
			return "slow", nil
		}
	}).Build()
	fast := ktest.Fake("fast", "fast")

	race := action.RaceAny("race", slow, fast).Build()
	ktest.Run(t, race, ctx, any(nil)).NoError().Equals("fast")

	latch.Signal() // cleanup
}

func TestSagaAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	var mu sync.Mutex
	var log []string
	appendLog := func(s string) { mu.Lock(); log = append(log, s); mu.Unlock() }

	step1 := action.DynamicSagaStep{
		Name: "s1",
		Do:   action.New("d1", func(_ context.Context, _ any) (any, error) { appendLog("do1"); return nil, nil }).Build(),
		Undo: action.New("u1", func(_ context.Context, _ any) (any, error) { appendLog("undo1"); return nil, nil }).Build(),
	}
	step2 := action.DynamicSagaStep{
		Name: "s2",
		Do:   action.New("d2", func(_ context.Context, _ any) (any, error) { appendLog("do2"); return nil, errors.New("boom") }).Build(),
		Undo: action.New("u2", func(_ context.Context, _ any) (any, error) { appendLog("undo2"); return nil, nil }).Build(),
	}

	saga := action.SagaAny("saga", []action.DynamicSagaStep{step1, step2}).Build()

	ktest.Run(t, saga, ctx, any(nil)).ErrorContains("failed at \"s2\"")

	// Verify LIFO rollback: Do1 -> Do2(fail) -> Undo1
	ktest.RequireEqual(t, log, []string{"do1", "do2", "undo1"})
}

func TestStateMachineAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	get := func(v any) string {
		m, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		s, _ := m["state"].(string)
		return s
	}
	set := func(v any, s string) {
		if m, ok := v.(map[string]any); ok {
			m["state"] = s
		}
	}
	transitions := map[string][]string{"init": {"running", "failed"}}

	t.Run("valid transition", func(t *testing.T) {
		act := action.New("run", func(_ context.Context, req any) (any, error) {
			m, ok := req.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected map[string]any, got %T", req)
			}
			m["state"] = "running"
			return m, nil
		}).Build()

		sm := action.StateMachineAny("sm", get, set, transitions, act).Build()
		raw := ktest.Run(t, sm, ctx, any(map[string]any{"state": "init"})).NoError().Value()
		res, ok := raw.(map[string]any)
		ktest.RequireCondition(t, ok, "expected map[string]any result")
		ktest.RequireEqual(t, res["state"], "running")
	})

	t.Run("invalid transition rejected and rolled back", func(t *testing.T) {
		act := action.New("invalid", func(_ context.Context, req any) (any, error) {
			m, ok := req.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected map[string]any, got %T", req)
			}
			m["state"] = "finished" // Not allowed from 'init'
			return m, nil
		}).Build()

		sm := action.StateMachineAny("sm", get, set, transitions, act).Build()
		req := map[string]any{"state": "init"}

		ktest.Run(t, sm, ctx, any(req)).ErrorKind(xerr.KindConflict).ErrorContains("rejected")

		// Ensure the original payload was rolled back by the Set closure
		ktest.RequireEqual(t, req["state"], "init")
	})
}

func TestTernaryAny_MapGateFalse(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	gate := action.New("gate", func(_ context.Context, _ any) (any, error) {
		return map[string]any{"match": false}, nil
	}).Build()
	thenAct := ktest.Fake("then", "then_result")
	elseAct := ktest.Fake("else", "else_result")

	ternary := action.TernaryAny("ternary", gate, thenAct, elseAct).Build()
	ktest.Run(t, ternary, ctx, any(nil)).NoError().Equals("else_result")
}

func TestTernaryAny_MapGateTrue(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	gate := action.New("gate", func(_ context.Context, _ any) (any, error) {
		return map[string]any{"match": true}, nil
	}).Build()
	thenAct := ktest.Fake("then", "then_result")
	elseAct := ktest.Fake("else", "else_result")

	ternary := action.TernaryAny("ternary", gate, thenAct, elseAct).Build()
	ktest.Run(t, ternary, ctx, any(nil)).NoError().Equals("then_result")
}

func TestTernaryAny_ApprovedFalseGate(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	gate := action.New("gate", func(_ context.Context, _ any) (any, error) {
		return map[string]any{"approved": false}, nil
	}).Build()
	thenAct := ktest.Fake("then", "then_result")
	elseAct := ktest.Fake("else", "else_result")

	ternary := action.TernaryAny("ternary", gate, thenAct, elseAct).Build()
	ktest.Run(t, ternary, ctx, any(nil)).NoError().Equals("else_result")
}

func TestRaceAny_ContextCancellationIsolatesGoroutines(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	// Slow action 1: blocks until context is canceled (or the 5s fail-safe fires).
	slow1 := action.New("slow1", func(c context.Context, _ any) (any, error) {
		select {
		case <-c.Done():
			return nil, c.Err()
		case <-time.After(5 * time.Second): // Fail-safe
			return nil, errors.New("timeout")
		}
	}).Build()

	// Slow action 2: same behavior.
	slow2 := action.New("slow2", func(c context.Context, _ any) (any, error) {
		select {
		case <-c.Done():
			return nil, c.Err()
		case <-time.After(5 * time.Second):
			return nil, errors.New("timeout")
		}
	}).Build()

	fast := ktest.Fake("fast", "fast_won")

	race := action.RaceAny("race", slow1, slow2, fast).Build()

	baseline := runtime.NumGoroutine()

	ktest.Run(t, race, ctx, any(nil)).NoError().Equals("fast_won")

	// Ensure slow goroutines exit immediately upon cancellation and don't leak.
	xtest.RequireNoGoroutineLeak(t, baseline, 1*time.Second)
}

func TestSwitchAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	actA := ktest.Fake("caseA", "result_A")
	actB := ktest.Fake("caseB", "result_B")
	actDef := ktest.Fake("default", "result_Default")

	cases := map[string]action.AnyAction{
		"A": actA,
		"B": actB,
	}

	selector := func(req any) string {
		if m, ok := req.(map[string]any); ok {
			if val, ok := m["type"].(string); ok {
				return val
			}
		}
		return ""
	}

	sw := action.SwitchAny("switch", selector, cases, actDef).Build()

	t.Run("matches exact case", func(t *testing.T) {
		ktest.Run(t, sw, ctx, any(map[string]any{"type": "A"})).NoError().Equals("result_A")
	})

	t.Run("falls back to default", func(t *testing.T) {
		ktest.Run(t, sw, ctx, any(map[string]any{"type": "UNKNOWN"})).NoError().Equals("result_Default")
	})

	t.Run("fails on missing default", func(t *testing.T) {
		swNoDef := action.SwitchAny("switch_nodef", selector, cases, nil).Build()
		ktest.Run(t, swNoDef, ctx, any(map[string]any{"type": "UNKNOWN"})).ErrorContains("unhandled case")
	})
}

func TestCatchAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	t.Run("bypasses recovery on success", func(t *testing.T) {
		okAct := ktest.Fake("ok", "success_value")
		recoverFn := func(_ context.Context, _ any, _ error) (any, error) {
			return "recovered", nil
		}

		catch := action.CatchAny("catch", okAct, recoverFn).Build()
		ktest.Run(t, catch, ctx, any(nil)).NoError().Equals("success_value")
	})

	t.Run("invokes recovery on error", func(t *testing.T) {
		errAct := ktest.FakeErr("err", xerr.Conflict("db_locked"))
		recoverFn := func(_ context.Context, _ any, err error) (any, error) {
			if xerr.KindFrom(err) == xerr.KindConflict {
				return "recovered_from_conflict", nil
			}
			return nil, err
		}

		catch := action.CatchAny("catch", errAct, recoverFn).Build()
		ktest.Run(t, catch, ctx, any(nil)).NoError().Equals("recovered_from_conflict")
	})
}

func TestTapAny(t *testing.T) {
	ctx, _ := ktest.Ctx(t)

	t.Run("executes side effect and returns input", func(t *testing.T) {
		sideEffectFired := false
		inspector := func(_ context.Context, req any) error {
			if s, ok := req.(string); ok && s == "trigger" {
				sideEffectFired = true
			}
			return nil
		}

		tap := action.TapAny("tap", inspector).Build()
		res := ktest.Run(t, tap, ctx, any("trigger")).NoError().Value()

		ktest.RequireCondition(t, sideEffectFired, "expected tap side-effect to fire")
		ktest.RequireEqual(t, res, "trigger")
	})

	t.Run("inspector error fails the action", func(t *testing.T) {
		inspector := func(_ context.Context, _ any) error {
			return errors.New("audit log full")
		}

		tap := action.TapAny("tap", inspector).Build()
		ktest.Run(t, tap, ctx, any("req")).ErrorContains("audit log full")
	})
}
