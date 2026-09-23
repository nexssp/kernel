// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestHookRegistry_RegisterAndResolve(t *testing.T) {
	t.Parallel()

	registry := action.NewHookRegistry()
	counter := ktest.NewCounter()

	err := registry.Register("counter", func() action.AnyHook {
		return action.AnyHook{
			Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
				counter.Inc()
				return ctx, nil
			},
		}
	})
	ktest.RequireNoError(t, err)

	hook, found := registry.NamedHook("counter")
	ktest.RequireCondition(t, found && hook.Before != nil, "expected registered hook with valid Before hook")

	_, _ = hook.Before(context.Background(), nil, &action.Meta{Name: "test"})
	counter.Require(t, 1)
}

func TestHookRegistry_ErrorsOnInvalidInput(t *testing.T) {
	t.Parallel()

	registry := action.NewHookRegistry()

	errorEmpty := registry.Register("", func() action.AnyHook { return action.AnyHook{} })
	ktest.RequireErrorIs(t, errorEmpty, action.ErrEmptyHookName)
	ktest.RequireErrorKind(t, errorEmpty, xerr.KindBadRequest)

	errorNil := registry.Register("valid", nil)
	ktest.RequireErrorIs(t, errorNil, action.ErrNilHookFactory)
	ktest.RequireErrorKind(t, errorNil, xerr.KindBadRequest)

	errorFirst := registry.Register("dup", func() action.AnyHook { return action.AnyHook{} })
	ktest.RequireNoError(t, errorFirst)

	errorDuplicate := registry.Register("dup", func() action.AnyHook { return action.AnyHook{} })
	ktest.RequireErrorIs(t, errorDuplicate, action.ErrDuplicateHook)
	ktest.RequireErrorKind(t, errorDuplicate, xerr.KindConflict)
}

func TestHookRegistry_UnknownName(t *testing.T) {
	t.Parallel()

	registry := action.NewHookRegistry()
	_, found := registry.NamedHook("does.not.exist")
	ktest.RequireCondition(t, !found, "expected unknown hook resolution to return false")
}

func TestHookRegistry_FactoryReturnsFreshInstance(t *testing.T) {
	t.Parallel()

	registry := action.NewHookRegistry()
	counter := ktest.NewCounter()

	_ = registry.Register("tick", func() action.AnyHook {
		counter.Inc()
		return action.AnyHook{}
	})

	_, _ = registry.NamedHook("tick")
	_, _ = registry.NamedHook("tick")
	_, _ = registry.NamedHook("tick")

	counter.Require(t, 3)
}

func TestHookRegistry_NamesAreSortedAndCopied(t *testing.T) {
	t.Parallel()

	registry := action.NewHookRegistry()
	for _, name := range []string{"zeta", "alpha", "mu"} {
		_ = registry.Register(name, func() action.AnyHook { return action.AnyHook{} })
	}

	names := registry.NamedHookNames()
	expectedNames := []string{"alpha", "mu", "zeta"}
	ktest.RequireEqual(t, names, expectedNames)

	// Defensively copied slice check
	names[0] = "mutated"
	ktest.RequireEqual(t, registry.NamedHookNames()[0], "alpha")
}

func TestHookRegistry_MustHelpersPanics(t *testing.T) {
	t.Parallel()

	t.Run("MustNamedHook panics on unknown", func(t *testing.T) {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected panic on missing hook")
			}
		}()
		_ = action.MustNamedHook("missing.hook.name")
	})

	t.Run("MustRegisterHook panics on error", func(t *testing.T) {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("expected panic on nil factory")
			}
		}()
		action.MustRegisterHook("invalid", nil)
	})
}

func TestHookRegistry_ConcurrentReadWriteSafety(t *testing.T) {
	t.Parallel()

	registry := action.NewHookRegistry()
	for index := range 10 {
		_ = registry.Register(fmt.Sprintf("hook.%d", index), func() action.AnyHook {
			return action.AnyHook{}
		})
	}

	const workers = 32
	const operationsPerWorker = 200

	xtest.RunParallel(t, workers, func(workerIndex int) error {
		for iteration := range operationsPerWorker {
			if workerIndex%4 == 0 {
				_ = registry.Register(fmt.Sprintf("dyn.%d.%d", workerIndex, iteration), func() action.AnyHook {
					return action.AnyHook{}
				})
			} else {
				lookupKey := fmt.Sprintf("hook.%d", (workerIndex+iteration)%10)
				_, _ = registry.NamedHook(lookupKey)
			}
		}
		return nil
	})
}

func TestHookRegistry_ZeroAllocOnLookup(t *testing.T) {
	registry := action.NewHookRegistry()
	_ = registry.Register("perf.hook", func() action.AnyHook {
		return action.AnyHook{}
	})

	// Pre-warm CPU cache
	for range 100 {
		_, _ = registry.NamedHook("perf.hook")
	}

	xtest.RequireZeroAlloc(t, 1000, func() {
		_, _ = registry.NamedHook("perf.hook")
	})
}
