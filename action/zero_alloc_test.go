package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func newZeroAllocAction() *action.BuiltAction[int, int] {
	return action.New("zeroalloc.action", func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	}).
		Tag("bench").
		HookBefore(func(ctx context.Context, _ int, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		}).
		Build()
}

func newZeroAllocAnyHookAction() *action.BuiltAction[int, int] {
	return action.New("zeroalloc.anyhook", func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	}).
		AnyHook(action.AnyHook{}).
		Build()
}

func TestActionDispatchZeroAlloc(t *testing.T) {
	// Nie używamy t.Parallel(), aby inne goroutines nie zaburzyły pomiaru alokacji.
	act := newZeroAllocAction()

	allocations := testing.AllocsPerRun(1000, func() {
		if _, err := act.Do(context.Background(), 42); err != nil {
			t.Fatal(err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations, got %.1f", allocations)
	}
}

func TestActionDispatchZeroAlloc_AnyHookSnapshot(t *testing.T) {
	// Nie używamy t.Parallel(), aby inne goroutines nie zaburzyły pomiaru alokacji.
	act := newZeroAllocAnyHookAction()

	allocations := testing.AllocsPerRun(1000, func() {
		if _, err := act.Do(context.Background(), 42); err != nil {
			t.Fatal(err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations, got %.1f", allocations)
	}
}
