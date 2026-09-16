package action_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

// =============================================================================
// 1. HAPPY PATH
// =============================================================================

func TestMustNewRegistry_Success(t *testing.T) {
	t.Parallel()

	act1 := action.New("user.create", dummyHandler).Build()
	act2 := action.New("user.delete", dummyHandler).Build()

	reg := action.MustNewRegistry(
		action.Library{
			Name:    "user_ops",
			Actions: []action.AnyAction{act1, act2},
			Aliases: []action.Alias{
				{Canonical: "user.create", Short: []string{"create_user"}},
			},
		},
		action.Of(
			action.New("system.ping", dummyHandler).Build(),
		),
	)

	if reg == nil {
		t.Fatal("expected non-nil registry")
	}

	if reg.Len() != 3 {
		t.Fatalf("expected 3 canonical actions, got %d", reg.Len())
	}

	if act, ok := reg.Get("user.create"); !ok || act == nil || act != act1 {
		t.Errorf("failed to resolve canonical action 'user.create'")
	}
	if act, ok := reg.Get("create_user"); !ok || act == nil || act != act1 {
		t.Errorf("failed to resolve alias 'create_user'")
	}
	if act, ok := reg.Get("system.ping"); !ok || act == nil {
		t.Errorf("failed to resolve inline action 'system.ping'")
	}
}

// =============================================================================
// 2. EXPLICIT ERROR PATHS (Configuration Validation)
// =============================================================================

func TestNewRegistry_ErrorsOnConflictingOverrides(t *testing.T) {
	t.Parallel()

	act := action.New("task.run", dummyHandler).Build()
	lib1 := action.Library{Name: "lib1", Actions: []action.AnyAction{act}, Overrides: []string{"task.run"}}
	lib2 := action.Library{Name: "lib2", Actions: []action.AnyAction{act}, Overrides: []string{"task.run"}}

	_, err := action.NewRegistry(lib1, lib2)
	if err == nil {
		t.Fatal("expected error on conflicting Overrides, got nil")
	}

	expected := `both declare Overrides for "task.run"`
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got %q", expected, err.Error())
	}
}

func TestNewRegistry_ErrorsOnUnnamedAction(t *testing.T) {
	t.Parallel()

	unnamed := action.New("", dummyHandler).Name("").Build()
	lib := action.Library{Name: "broken", Actions: []action.AnyAction{unnamed}}

	_, err := action.NewRegistry(lib)
	if err == nil {
		t.Fatal("expected error on unnamed action, got nil")
	}

	expected := `library "broken" contains an action with no name`
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got %q", expected, err.Error())
	}
}

func TestNewRegistry_ErrorsOnDuplicateActionInSameLibrary(t *testing.T) {
	t.Parallel()

	act1 := action.New("item.save", dummyHandler).Build()
	act2 := action.New("item.save", dummyHandler).Build()
	lib := action.Library{Name: "store", Actions: []action.AnyAction{act1, act2}}

	_, err := action.NewRegistry(lib)
	if err == nil {
		t.Fatal("expected error on duplicate action in same library, got nil")
	}

	expected := `library "store" declares "item.save" more than once`
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got %q", expected, err.Error())
	}
}

func TestNewRegistry_ErrorsOnCollisionWithoutOverride(t *testing.T) {
	t.Parallel()

	actA := action.New("auth.login", dummyHandler).Build()
	actB := action.New("auth.login", dummyHandler).Build()
	lib1 := action.Library{Name: "base", Actions: []action.AnyAction{actA}}
	lib2 := action.Library{Name: "custom", Actions: []action.AnyAction{actB}}

	_, err := action.NewRegistry(lib1, lib2)
	if err == nil {
		t.Fatal("expected error on collision without override, got nil")
	}

	expected := `action: "auth.login" declared by both "base" and "custom"`
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got %q", expected, err.Error())
	}
}

func TestNewRegistry_RejectsActionReuseWithHooks(t *testing.T) {
	t.Parallel()

	act := action.New("task.run", dummyHandler).Build()
	hook := action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		},
	}

	lib := action.Library{
		Name:    "with_hooks",
		Actions: []action.AnyAction{act},
		Hooks:   []action.AnyHook{hook},
	}

	if _, err := action.NewRegistry(lib); err != nil {
		t.Fatalf("first registry must succeed: %v", err)
	}

	_, err := action.NewRegistry(lib)
	if err == nil {
		t.Fatal("expected error on second registry with same action instance (and hooks), got nil")
	}

	if !strings.Contains(err.Error(), "task.run") {
		t.Fatalf("expected action name 'task.run' in error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "already received hooks from a previous NewRegistry call") {
		t.Fatalf("expected specific error message for hook accumulation, got %q", err.Error())
	}
}

func TestNewRegistry_AllowsActionReuseWithoutHooks(t *testing.T) {
	t.Parallel()

	act := action.New("task.run", dummyHandler).Build()
	libNoHooks := action.Library{Name: "no_hooks", Actions: []action.AnyAction{act}}

	if _, err := action.NewRegistry(libNoHooks); err != nil {
		t.Fatalf("first registry must succeed: %v", err)
	}
	if _, err := action.NewRegistry(libNoHooks); err != nil {
		t.Fatalf("second registry must also succeed (no hooks): %v", err)
	}

	// Akcja nie została oznaczona przez poprzednie rejestracje (nie było hooków),
	// więc wciąż można jej przypisać hooki. To jest obserwowalny efekt kontraktu
	// zamiast zaglądania w nieeksportowane pole.
	libWithHooks := action.Library{
		Name:    "with_hooks",
		Actions: []action.AnyAction{act},
		Hooks:   []action.AnyHook{{}},
	}
	if _, err := action.NewRegistry(libWithHooks); err != nil {
		t.Fatalf("action should still be claimable after hook-less registries: %v", err)
	}
}

// =============================================================================
// 3. MUST-NEW-REGISTRY PANICS (Fail-Fast Boot)
// =============================================================================

func TestMustNewRegistry_PanicsOnCollisionWithoutOverride(t *testing.T) {
	t.Parallel()

	actA := action.New("data.fetch", dummyHandler).Build()
	actB := action.New("data.fetch", dummyHandler).Build()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected MustNewRegistry to panic on action collision, but it returned normally")
		}
		if !strings.Contains(fmt.Sprint(r), "data.fetch") {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()

	_ = action.MustNewRegistry(
		action.Library{Name: "L1", Actions: []action.AnyAction{actA}},
		action.Library{Name: "L2", Actions: []action.AnyAction{actB}},
	)
}

func TestMustNewRegistry_PanicsOnEmptyActionName(t *testing.T) {
	t.Parallel()

	unnamed := action.New("", dummyHandler).Name("").Build()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected MustNewRegistry to panic on unnamed action, but it returned normally")
		}
		if !strings.Contains(fmt.Sprint(r), "contains an action with no name") {
			t.Errorf("unexpected panic message: %v", r)
		}
	}()

	_ = action.MustNewRegistry(action.Of(unnamed))
}

// =============================================================================
// 4. EDGE CASES (Silent Conflict Resolution & Safeties)
// =============================================================================

func TestNewRegistry_SkipsNilAction(t *testing.T) {
	t.Parallel()

	valid := action.New("valid.act", dummyHandler).Build()
	lib := action.Library{Name: "mixed", Actions: []action.AnyAction{nil, valid, nil}}

	reg, err := action.NewRegistry(lib)
	if err != nil {
		t.Fatalf("unexpected error with nil actions: %v", err)
	}
	if reg.Len() != 1 {
		t.Fatalf("expected 1 action, got %d", reg.Len())
	}
}

func TestNewRegistry_IgnoresDeadAlias(t *testing.T) {
	t.Parallel()

	act := action.New("real.op", dummyHandler).Build()
	lib := action.Library{
		Name:    "lib",
		Actions: []action.AnyAction{act},
		Aliases: []action.Alias{
			{Canonical: "non.existent.op", Short: []string{"ghost"}},
		},
	}

	reg, err := action.NewRegistry(lib)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := reg.Get("ghost"); ok {
		t.Errorf("ghost alias should not resolve to anything")
	}
}

func TestNewRegistry_AliasPrecedence(t *testing.T) {
	t.Parallel()

	act1 := action.New("canonical.first", dummyHandler).Build()
	act2 := action.New("canonical.second", dummyHandler).Build()

	lib1 := action.Library{
		Name:    "L1",
		Actions: []action.AnyAction{act1},
		Aliases: []action.Alias{{Canonical: "canonical.first", Short: []string{"run"}}},
	}
	lib2 := action.Library{
		Name:    "L2",
		Actions: []action.AnyAction{act2},
		Aliases: []action.Alias{{Canonical: "canonical.second", Short: []string{"run"}}},
	}

	reg := action.MustNewRegistry(lib1, lib2)

	got, ok := reg.Get("run")
	if !ok || got != act1 {
		t.Errorf("expected earlier library (L1) to win alias resolution for 'run'")
	}
}

func TestRegistry_NilReceiverSafety(t *testing.T) {
	t.Parallel()

	var reg *action.Registry

	if act, ok := reg.Get("anything"); ok || act != nil {
		t.Errorf("nil registry Get() should return (nil, false)")
	}
	if acts := reg.Actions(); acts != nil {
		t.Errorf("nil registry Actions() should return nil")
	}
	if l := reg.Len(); l != 0 {
		t.Errorf("nil registry Len() should return 0")
	}
}

// =============================================================================
// 5. ZERO-ALLOC TEST & BENCHMARKS (O(1) read path guarantees)
// =============================================================================

func TestRegistry_ZeroAllocations(t *testing.T) {
	act1 := action.New("db.query", dummyHandler).Build()
	act2 := action.New("cache.get", dummyHandler).Build()

	reg := action.MustNewRegistry(action.Library{
		Name:    "perf",
		Actions: []action.AnyAction{act1, act2},
		Aliases: []action.Alias{
			{Canonical: "db.query", Short: []string{"query", "q"}},
		},
	})

	allocsGet := testing.AllocsPerRun(1000, func() {
		a, ok := reg.Get("db.query")
		if !ok || a == nil {
			t.Fail()
		}
	})
	if allocsGet != 0 {
		t.Errorf("expected 0 allocs for Get(canonical), got %f", allocsGet)
	}

	allocsAlias := testing.AllocsPerRun(1000, func() {
		a, ok := reg.Get("query")
		if !ok || a == nil {
			t.Fail()
		}
	})
	if allocsAlias != 0 {
		t.Errorf("expected 0 allocs for Get(alias), got %f", allocsAlias)
	}

	allocsActions := testing.AllocsPerRun(1000, func() {
		acts := reg.Actions()
		if len(acts) != 2 {
			t.Fail()
		}
	})
	if allocsActions != 0 {
		t.Errorf("expected 0 allocs for Actions(), got %f", allocsActions)
	}

	allocsLen := testing.AllocsPerRun(1000, func() {
		if reg.Len() != 2 {
			t.Fail()
		}
	})
	if allocsLen != 0 {
		t.Errorf("expected 0 allocs for Len(), got %f", allocsLen)
	}
}

func BenchmarkRegistry_Get_Canonical(b *testing.B) {
	act := action.New("bench.op", dummyHandler).Build()
	reg := action.MustNewRegistry(action.Library{Name: "bench", Actions: []action.AnyAction{act}})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Get("bench.op")
	}
}

func BenchmarkRegistry_Get_Alias(b *testing.B) {
	act := action.New("bench.op", dummyHandler).Build()
	reg := action.MustNewRegistry(action.Library{
		Name:    "bench",
		Actions: []action.AnyAction{act},
		Aliases: []action.Alias{{Canonical: "bench.op", Short: []string{"b"}}},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reg.Get("b")
	}
}

func BenchmarkRegistry_Actions(b *testing.B) {
	act := action.New("bench.op", dummyHandler).Build()
	reg := action.MustNewRegistry(action.Library{Name: "bench", Actions: []action.AnyAction{act}})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reg.Actions()
	}
}

func dummyHandler(ctx context.Context, req any) (any, error) {
	return req, nil
}
