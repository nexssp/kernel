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

// TestActionDispatchZeroAlloc verifies that basic action execution has 0 heap allocations.
func TestActionDispatchZeroAlloc(t *testing.T) {
	act := newZeroAllocAction()
	ctx := context.Background()

	allocations := testing.AllocsPerRun(1000, func() {
		if _, err := act.Do(ctx, 42); err != nil {
			t.Fatal(err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations for single Action.Do, got %.1f", allocations)
	}
}

// TestActionDispatchZeroAlloc_AnyHookSnapshot verifies hook snapshotting introduces 0 heap allocations.
func TestActionDispatchZeroAlloc_AnyHookSnapshot(t *testing.T) {
	act := newZeroAllocAnyHookAction()
	ctx := context.Background()

	allocations := testing.AllocsPerRun(1000, func() {
		if _, err := act.Do(ctx, 42); err != nil {
			t.Fatal(err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations for Action.Do with AnyHook, got %.1f", allocations)
	}
}

// TestPipeZeroAlloc proves that composer.Pipe passes stack values with 0 heap allocations.
func TestPipeZeroAlloc(t *testing.T) {
	act1 := action.New("step1", func(_ context.Context, n int) (int, error) {
		return n + 1, nil
	}).Build()

	act2 := action.New("step2", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Build()

	pipe := action.Pipe("zero_alloc_pipe", act1, act2).Build()
	ctx := context.Background()

	allocations := testing.AllocsPerRun(1000, func() {
		res, err := pipe.Do(ctx, 20)
		if err != nil || res != 42 {
			t.Fatalf("unexpected pipe outcome: res=%d err=%v", res, err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations for composer.Pipe, got %.1f", allocations)
	}
}

// TestPipeWithZeroAlloc proves that composer.PipeWith performs inline struct transformation with 0 heap allocations.
func TestPipeWithZeroAlloc(t *testing.T) {
	type SourceDTO struct {
		Count int
	}
	type TargetDTO struct {
		Doubled int
	}

	act1 := action.New("step1", func(_ context.Context, s SourceDTO) (SourceDTO, error) {
		return s, nil
	}).Build()

	act2 := action.New("step2", func(_ context.Context, tgt TargetDTO) (int, error) {
		return tgt.Doubled, nil
	}).Build()

	pipeWith := action.PipeWith("zero_alloc_pipe_with",
		act1,
		func(_ context.Context, s SourceDTO) (TargetDTO, error) {
			return TargetDTO{Doubled: s.Count * 2}, nil
		},
		act2,
	).Build()

	ctx := context.Background()
	req := SourceDTO{Count: 21}

	allocations := testing.AllocsPerRun(1000, func() {
		res, err := pipeWith.Do(ctx, req)
		if err != nil || res != 42 {
			t.Fatalf("unexpected pipe outcome: res=%d err=%v", res, err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations for composer.PipeWith, got %.1f", allocations)
	}
}

// TestChainZeroAlloc proves that sequential chaining has 0 heap allocations.
func TestChainZeroAlloc(t *testing.T) {
	add := action.New("add", func(_ context.Context, n int) (int, error) { return n + 1, nil })
	mul := action.New("mul", func(_ context.Context, n int) (int, error) { return n * 2, nil })

	chain := action.Chain("zero_alloc_chain", add, mul).Build()
	ctx := context.Background()

	allocations := testing.AllocsPerRun(1000, func() {
		res, err := chain.Do(ctx, 5)
		if err != nil || res != 12 {
			t.Fatalf("unexpected chain outcome: res=%d err=%v", res, err)
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations for composer.Chain, got %.1f", allocations)
	}
}

// TestDoAny_PointerFastPath_ZeroAlloc proves that InvokeAny with pointer DTOs
// incurs 0 heap allocations on interface invocation.
func TestDoAny_PointerFastPath_ZeroAlloc(t *testing.T) {
	type CodePayload struct {
		Lines int
	}

	act := action.New("code.process", func(_ context.Context, req *CodePayload) (*CodePayload, error) {
		req.Lines = 20
		return req, nil
	}).Build()

	ctx := context.Background()
	payload := &CodePayload{Lines: 10}

	allocations := testing.AllocsPerRun(1000, func() {
		res, err := action.InvokeAny(ctx, act, payload)
		if err != nil {
			t.Fatal(err)
		}
		if res.(*CodePayload).Lines != 20 {
			t.Fatal("invalid output")
		}
	})

	if allocations != 0 {
		t.Fatalf("expected 0 allocations for InvokeAny with pointer DTO fast-path, got %.1f", allocations)
	}
}
