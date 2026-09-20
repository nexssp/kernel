package action_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

// ── Behavior ─────────────────────────────────────────────────────────────────

func TestOnBuild_NilAlwaysAttaches(t *testing.T) {
	act := action.New("onbuild.nil", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(action.AnyHook{}).Build()

	if got := len(act.GetAnyHooks()); got != 1 {
		t.Fatalf("nil OnBuild must attach unconditionally, got %d hooks", got)
	}
}

func TestOnBuild_TrueAttachesInBuild(t *testing.T) {
	act := action.New("onbuild.true", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return true },
	}).Build()

	if got := len(act.GetAnyHooks()); got != 1 {
		t.Fatalf("OnBuild=true must attach, got %d hooks", got)
	}
}

func TestOnBuild_FalseDropsInBuild(t *testing.T) {
	act := action.New("onbuild.false", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return false },
	}).Build()

	if got := len(act.GetAnyHooks()); got != 0 {
		t.Fatalf("OnBuild=false must drop, got %d hooks", got)
	}
}

func TestOnBuild_FalseDropsInAddAnyHook(t *testing.T) {
	act := action.New("onbuild.addhook", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Build()

	act.AddAnyHook(action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return false },
	})

	if got := len(act.GetAnyHooks()); got != 0 {
		t.Fatalf("OnBuild=false must drop in AddAnyHook, got %d hooks", got)
	}
}

func TestOnBuild_AppliesViaLibraryHooks(t *testing.T) {
	tagged := action.New("lib.keep", func(_ context.Context, _ struct{ A int }) (int, error) {
		return 0, nil
	}).Build()
	plain := action.New("lib.drop", func(_ context.Context, _ int) (int, error) {
		return 0, nil
	}).Build()

	hook := action.AnyHook{
		OnBuild: func(_ *action.Meta, reqType, _ reflect.Type) bool {
			return reqType == reflect.TypeFor[struct{ A int }]()
		},
	}

	// Use the new ApplyHooks pattern instead of Library.Hooks
	wrappedActions := action.ApplyHooks([]action.AnyAction{tagged, plain}, hook)

	reg := action.MustNewRegistry(action.Library{
		Name:    "onbuild.lib",
		Actions: wrappedActions,
	})

	kept, _ := reg.Get("lib.keep")
	if got := len(kept.GetAnyHooks()); got != 1 {
		t.Fatalf("lib.keep expected 1 hook, got %d", got)
	}
	dropped, _ := reg.Get("lib.drop")
	if got := len(dropped.GetAnyHooks()); got != 0 {
		t.Fatalf("lib.drop expected 0 hooks, got %d", got)
	}
}

// ── Signature: meta, reqType, resType ────────────────────────────────────────

func TestOnBuild_ReceivesMetaAndTypes(t *testing.T) {
	type typedReq struct{ A int }
	type typedRes struct{ B string }

	var (
		gotMeta *action.Meta
		gotReq  reflect.Type
		gotRes  reflect.Type
	)

	act := action.New("onbuild.types", func(_ context.Context, _ typedReq) (typedRes, error) {
		return typedRes{}, nil
	}).AnyHook(action.AnyHook{
		OnBuild: func(meta *action.Meta, reqType, resType reflect.Type) bool {
			gotMeta, gotReq, gotRes = meta, reqType, resType
			return true
		},
	}).Build()

	if gotMeta == nil || gotMeta.Name != "onbuild.types" {
		t.Fatalf("meta = %+v, want Name=onbuild.types", gotMeta)
	}
	if gotReq != reflect.TypeFor[typedReq]() {
		t.Fatalf("reqType = %v, want %v", gotReq, reflect.TypeFor[typedReq]())
	}
	if gotRes != reflect.TypeFor[typedRes]() {
		t.Fatalf("resType = %v, want %v", gotRes, reflect.TypeFor[typedRes]())
	}
	_ = act
}

func TestOnBuild_ReceivesPointerTypes(t *testing.T) {
	var gotReq, gotRes reflect.Type

	_ = action.New("onbuild.ptrs", func(_ context.Context, _ *int) (*string, error) {
		return nil, nil
	}).AnyHook(action.AnyHook{
		OnBuild: func(_ *action.Meta, reqType, resType reflect.Type) bool {
			gotReq, gotRes = reqType, resType
			return true
		},
	}).Build()

	if gotReq != reflect.TypeFor[*int]() {
		t.Fatalf("reqType = %v, want *int", gotReq)
	}
	if gotRes != reflect.TypeFor[*string]() {
		t.Fatalf("resType = %v, want *string", gotRes)
	}
}

// ── Call counts ──────────────────────────────────────────────────────────────

func TestOnBuild_CalledOncePerHookInBuild(t *testing.T) {
	var calls int
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { calls++; return true },
	}
	_ = action.New("onbuild.once", func(_ context.Context, _ int) (int, error) {
		return 0, nil
	}).AnyHook(hook).Build()

	if calls != 1 {
		t.Fatalf("OnBuild called %d times, want 1", calls)
	}
}

func TestOnBuild_CalledOncePerHookInAddAnyHook(t *testing.T) {
	var calls int
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { calls++; return true },
	}
	act := action.New("onbuild.once.addhook", func(_ context.Context, _ int) (int, error) {
		return 0, nil
	}).Build()
	act.AddAnyHook(hook)

	if calls != 1 {
		t.Fatalf("OnBuild called %d times, want 1", calls)
	}
}

func TestOnBuild_CalledOncePerActionInRegistry(t *testing.T) {
	var calls int
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { calls++; return true },
	}

	rawActions := []action.AnyAction{
		action.New("r.a", func(_ context.Context, _ int) (int, error) { return 0, nil }).Build(),
		action.New("r.b", func(_ context.Context, _ string) (string, error) { return "", nil }).Build(),
	}

	// Apply hooks explicitly
	wrappedActions := action.ApplyHooks(rawActions, hook)

	_ = action.MustNewRegistry(action.Library{
		Name:    "onbuild.registry",
		Actions: wrappedActions,
	})

	if calls != 2 {
		t.Fatalf("OnBuild called %d times, want 2 (once per action)", calls)
	}
}

// ── Runtime isolation ────────────────────────────────────────────────────────

func TestOnBuild_FilteredHookNeverRunsInDo(t *testing.T) {
	var beforeCalls, afterCalls int
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return false },
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			beforeCalls++
			return ctx, nil
		},
		After: func(context.Context, any, any, error, *action.Meta) {
			afterCalls++
		},
	}

	act := action.New("onbuild.isolated", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(hook).Build()

	for i := range 5 {
		if _, err := act.Do(context.Background(), i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if beforeCalls != 0 || afterCalls != 0 {
		t.Fatalf("filtered hook ran: before=%d after=%d", beforeCalls, afterCalls)
	}
}

// ── Panic behavior (fail fast at boot) ───────────────────────────────────────

func TestOnBuild_PanicPropagates(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic from OnBuild to propagate")
		}
		if !strings.Contains(fmt.Sprint(r), "boom") {
			t.Fatalf("unexpected panic value: %v", r)
		}
	}()

	_ = action.New("onbuild.panic", func(_ context.Context, _ int) (int, error) {
		return 0, nil
	}).AnyHook(action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool {
			panic("boom")
		},
	}).Build()
}

// ── Zero-alloc hot path ──────────────────────────────────────────────────────

func TestOnBuild_ZeroAlloc_NoHooks(t *testing.T) {
	act := action.New("onbuild.hot.nohooks", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Build()

	ctx := context.Background()
	allocs := testing.AllocsPerRun(10_000, func() {
		if _, err := act.Do(ctx, 42); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op with no hooks, got %.2f", allocs)
	}
}

func TestOnBuild_ZeroAlloc_AcceptedHook(t *testing.T) {
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return true },
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		},
	}

	act := action.New("onbuild.hot.accepted", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(hook).Build()

	ctx := context.Background()
	allocs := testing.AllocsPerRun(10_000, func() {
		if _, err := act.Do(ctx, 42); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op with accepted hook, got %.2f", allocs)
	}
}

func TestOnBuild_ZeroAlloc_FilteredHook(t *testing.T) {
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return false },
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		},
	}

	act := action.New("onbuild.hot.filtered", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(hook).Build()

	ctx := context.Background()
	allocs := testing.AllocsPerRun(10_000, func() {
		if _, err := act.Do(ctx, 42); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op after filter drop, got %.2f", allocs)
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkOnBuild_Do_NoHooks(b *testing.B) {
	act := action.New("bench.nohooks", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = act.Do(ctx, 42)
	}
}

func BenchmarkOnBuild_Do_AcceptedHook(b *testing.B) {
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return true },
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		},
	}
	act := action.New("bench.accepted", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(hook).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = act.Do(ctx, 42)
	}
}

func BenchmarkOnBuild_Do_FilteredHook(b *testing.B) {
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return false },
	}
	act := action.New("bench.filtered", func(_ context.Context, n int) (int, error) {
		return n, nil
	}).AnyHook(hook).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = act.Do(ctx, 42)
	}
}

func BenchmarkOnBuild_Build_WithFilter(b *testing.B) {
	hook := action.AnyHook{
		OnBuild: func(*action.Meta, reflect.Type, reflect.Type) bool { return true },
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = action.New("bench.build", func(_ context.Context, n int) (int, error) {
			return n, nil
		}).AnyHook(hook).Build()
	}
}
