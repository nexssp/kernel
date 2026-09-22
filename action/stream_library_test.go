package action_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

// ── Helpers ─────────────────────────────────────────────────────────────

func drainAnyStream(s action.AnyStream) error {
	var firstErr error
	s(func(_ any, err error) bool {
		if err != nil {
			firstErr = err
			return false
		}
		return true
	})
	return firstErr
}

func intSource(name string, n int) *action.StreamAction[struct{}, int] {
	return action.NewStream(name, func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			for i := range n {
				if !yield(i, nil) {
					return
				}
			}
		}, nil
	})
}

func noopOperator(name string) action.NamedOperator {
	return action.NamedOperator{
		Name:    name,
		InType:  reflect.TypeFor[int](),
		OutType: reflect.TypeFor[int](),
		Build: func(_ map[string]any) (action.StreamOperator, error) {
			return action.NewTypedStreamOperator[int, int](name,
				func(up iter.Seq2[int, error]) iter.Seq2[int, error] { return up },
			), nil
		},
	}
}

// ── StreamAction anyHooks ───────────────────────────────────────────────

func TestStreamAction_AddAnyHook_FiresBefore(t *testing.T) {
	t.Parallel()

	before := ktest.NewCounter()

	s := intSource("t.stream", 3)
	s.AddAnyHook(action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			before.Inc()
			return ctx, nil
		},
	})

	seq, err := s.Do(context.Background(), struct{}{})
	ktest.RequireNoError(t, err)

	for range seq {
		continue
	}

	before.Require(t, 1)
}

func TestStreamAction_AddAnyHook_FiresAfterOnFullDrain(t *testing.T) {
	t.Parallel()

	after := ktest.NewCounter()

	s := intSource("t.stream", 3)
	s.AddAnyHook(action.AnyHook{
		After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
			after.Inc()
		},
	})

	seq, err := s.Do(context.Background(), struct{}{})
	ktest.RequireNoError(t, err)

	for range seq {
		continue
	}

	after.Require(t, 1)
}

func TestStreamAction_AddAnyHook_FiresAfterOnEarlyExit(t *testing.T) {
	t.Parallel()

	after := ktest.NewCounter()

	s := intSource("t.stream", 10)
	s.AddAnyHook(action.AnyHook{
		After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
			after.Inc()
		},
	})

	seq, err := s.Do(context.Background(), struct{}{})
	ktest.RequireNoError(t, err)

	for range seq {
		break
	}

	after.Require(t, 1)
}

func TestStreamAction_AddAnyHook_FiresAfterOnHandlerError(t *testing.T) {
	t.Parallel()

	after := ktest.NewCounter()

	s := action.NewStream("t.stream",
		func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
			return nil, errors.New("boom")
		})

	s.AddAnyHook(action.AnyHook{
		After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
			after.Inc()
		},
	})

	_, err := s.Do(context.Background(), struct{}{})
	ktest.RequireErrorContains(t, err, "boom")

	after.Require(t, 1)
}

// ── StreamAction CloneWithHooks ─────────────────────────────────────────

func TestStreamAction_CloneWithHooks_DoesNotMutateOriginal(t *testing.T) {
	t.Parallel()

	cloneFires := ktest.NewCounter()

	s := intSource("t.stream", 1)
	ktest.RequireEqual(t, len(s.GetAnyHooks()), 0)

	cloned := s.CloneWithHooks(action.AnyHook{
		After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
			cloneFires.Inc()
		},
	})
	ktest.RequireCondition(t, cloned != nil, "CloneWithHooks returned nil")

	// Drain original — clone hook must NOT fire.
	seq, err := s.Do(context.Background(), struct{}{})
	ktest.RequireNoError(t, err)
	for range seq {
		continue
	}
	cloneFires.Require(t, 0)

	// Drain clone — its hook MUST fire.
	anyStream, err := cloned.DoStreamAny(context.Background(), struct{}{})
	ktest.RequireNoError(t, err)
	ktest.RequireNoError(t, drainAnyStream(anyStream))

	cloneFires.Require(t, 1)

	// Original must remain unmutated.
	ktest.RequireEqual(t, len(s.GetAnyHooks()), 0)
}

// ── Library.Hooks on Actions ────────────────────────────────────────────

func TestLibrary_Hooks_ApplyToActions(t *testing.T) {
	t.Parallel()

	after := ktest.NewCounter()

	act := action.New("plain", func(_ context.Context, in string) (string, error) {
		return in, nil
	}).Build()

	reg, err := action.NewRegistry(action.Library{
		Name:    "test",
		Actions: []action.AnyAction{act},
		Hooks: []action.AnyHook{
			{
				After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
					after.Inc()
				},
			},
		},
	})
	ktest.RequireNoError(t, err)

	got, ok := reg.Get("plain")
	ktest.RequireCondition(t, ok, "action not registered")

	_, err = got.DoAny(context.Background(), "x")
	ktest.RequireNoError(t, err)

	after.Require(t, 1)

	// Original must be untouched.
	ktest.RequireEqual(t, len(act.GetAnyHooks()), 0)
}

// ── Library.Hooks on Sources ────────────────────────────────────────────

func TestLibrary_Hooks_ApplyToSources(t *testing.T) {
	t.Parallel()

	before := ktest.NewCounter()
	after := ktest.NewCounter()

	src := intSource("fs.walk", 3)

	reg, err := action.NewRegistry(action.Library{
		Name:    "test",
		Sources: []action.AnyStreamAction{src},
		Hooks: []action.AnyHook{
			{
				Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
					before.Inc()
					return ctx, nil
				},
				After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
					after.Inc()
				},
			},
		},
	})
	ktest.RequireNoError(t, err)

	got, ok := reg.GetStream("fs.walk")
	ktest.RequireCondition(t, ok, "source not registered")

	anyStream, err := got.DoStreamAny(context.Background(), struct{}{})
	ktest.RequireNoError(t, err)
	ktest.RequireNoError(t, drainAnyStream(anyStream))

	before.Require(t, 1)
	after.Require(t, 1)

	// Original must be untouched.
	ktest.RequireEqual(t, len(src.GetAnyHooks()), 0)
}

// ── Library.Hooks must NOT apply to Operators ───────────────────────────

func TestLibrary_Hooks_NotAppliedToOperators(t *testing.T) {
	t.Parallel()

	fired := ktest.NewCounter()

	reg, err := action.NewRegistry(action.Library{
		Name:      "test",
		Operators: []action.NamedOperator{noopOperator("op")},
		Hooks: []action.AnyHook{
			{
				Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
					fired.Inc()
					return ctx, nil
				},
			},
		},
	})
	ktest.RequireNoError(t, err)

	got, ok := reg.GetOperator("op")
	ktest.RequireCondition(t, ok, "operator not registered")

	_, err = got.Build(map[string]any{})
	ktest.RequireNoError(t, err)

	fired.Require(t, 0)
}

// ── Alias precedence ────────────────────────────────────────────────────

func TestRegistry_AliasDoesNotOverrideExistingAction(t *testing.T) {
	t.Parallel()

	canonical := action.New("execute", func(_ context.Context, in string) (string, error) {
		return "execute:" + in, nil
	}).Build()

	other := action.New("other", func(_ context.Context, in string) (string, error) {
		return "other:" + in, nil
	}).Build()

	reg, err := action.NewRegistry(
		action.Library{Name: "L1", Actions: []action.AnyAction{canonical, other}},
		action.Library{
			Name:    "L2",
			Aliases: []action.Alias{{Canonical: "other", Short: []string{"execute"}}},
		},
	)
	ktest.RequireNoError(t, err)

	got, ok := reg.Get("execute")
	ktest.RequireCondition(t, ok, "execute not registered")

	out, err := got.DoAny(context.Background(), "x")
	ktest.RequireNoError(t, err)
	ktest.RequireEqual(t, out, "execute:x")
}

func TestRegistry_AliasSkippedWhenCanonicalMissing(t *testing.T) {
	t.Parallel()

	reg, err := action.NewRegistry(action.Library{
		Name:    "L1",
		Aliases: []action.Alias{{Canonical: "missing", Short: []string{"x"}}},
	})
	ktest.RequireNoError(t, err)

	_, ok := reg.Get("x")
	ktest.RequireCondition(t, !ok, "alias registered despite missing canonical")
}

// ── Concurrency smoke test ──────────────────────────────────────────────

func TestLibrary_Hooks_ConcurrentDrainIsRaceFree(t *testing.T) {
	t.Parallel()

	src := intSource("fs.walk", 20)

	reg, err := action.NewRegistry(action.Library{
		Name:    "test",
		Sources: []action.AnyStreamAction{src},
		Hooks: []action.AnyHook{
			{
				Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
					return ctx, nil
				},
				After: func(_ context.Context, _, _ any, _ error, _ *action.Meta) {
				},
			},
		},
	})
	ktest.RequireNoError(t, err)

	got, ok := reg.GetStream("fs.walk")
	ktest.RequireCondition(t, ok, "source not registered")

	xtest.RunParallel(t, 16, func(_ int) error {
		anyStream, err := got.DoStreamAny(context.Background(), struct{}{})
		if err != nil {
			return err
		}
		return drainAnyStream(anyStream)
	})
}
