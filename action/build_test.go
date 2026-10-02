package action_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest/ktest"
)

// =============================================================================
// 1. HAPPY PATH
// =============================================================================

func TestMustNewRegistry_Success(t *testing.T) {
	t.Parallel()

	act1 := ktest.Echo[any]("user.create").Build()
	act2 := ktest.Echo[any]("user.delete").Build()

	reg := action.MustNewRegistry(
		action.Library{
			Name:    "user_ops",
			Actions: []action.AnyAction{act1, act2},
			Aliases: []action.Alias{
				{Canonical: "user.create", Short: []string{"create_user"}},
			},
		},
		action.Of(
			ktest.Echo[any]("system.ping").Build(),
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

	act := ktest.Echo[any]("task.run").Build()
	lib1 := action.Library{Name: "lib1", Actions: []action.AnyAction{act}}
	lib2 := action.Library{Name: "lib2", Actions: []action.AnyAction{act}}

	_, err := action.NewRegistry(lib1, lib2)
	if err == nil {
		t.Fatal("expected error on conflicting Overrides, got nil")
	}

	expected := `"task.run" declared by both "lib1" and "lib2"`
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error containing %q, got %q", expected, err.Error())
	}
}

func TestNewRegistry_ErrorsOnUnnamedAction(t *testing.T) {
	t.Parallel()

	unnamed := ktest.Echo[any]("").Name("").Build()
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

	act1 := ktest.Echo[any]("item.save").Build()
	act2 := ktest.Echo[any]("item.save").Build()
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

func TestNewRegistry_AllowsActionReuseAcrossMultipleRegistries(t *testing.T) {
	t.Parallel()

	// 1. Create a core business action ONCE.
	coreAction := ktest.Echo[any]("task.run").Build()

	// 2. Create a hook for the public API registry
	publicHook := action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		},
	}

	// 3. Create a hook for the internal admin registry
	internalHook := action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		},
	}

	// 4. Safely clone and apply hooks for two completely independent registries
	publicLib := action.Library{
		Name:    "public_api",
		Actions: action.ApplyHooks([]action.AnyAction{coreAction}, publicHook),
	}

	internalLib := action.Library{
		Name:    "internal_api",
		Actions: action.ApplyHooks([]action.AnyAction{coreAction}, internalHook),
	}

	// Both registries build perfectly. No panics. The coreAction is cloned safely.
	if _, err := action.NewRegistry(publicLib); err != nil {
		t.Fatalf("failed to build public registry: %v", err)
	}
	if _, err := action.NewRegistry(internalLib); err != nil {
		t.Fatalf("failed to build internal registry: %v", err)
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

func TestNewRegistry_ErrorsOnMissingAliasTarget(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected missing alias target error")
	}
	if reg != nil {
		t.Fatal("invalid alias must not return a partially built registry")
	}
	for _, part := range []string{`missing canonical "non.existent.op"`, `kind canonical`, `owner unknown`} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err, part)
		}
	}
}

func TestNewRegistry_ErrorsOnDuplicateAliasNames(t *testing.T) {
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

	reg, err := action.NewRegistry(lib1, lib2)
	if err == nil {
		t.Fatal("expected duplicate alias name error")
	}
	if reg != nil {
		t.Fatal("duplicate aliases must not return a partially built registry")
	}
	for _, part := range []string{`duplicate alias name "run"`, `"L1"`, `"L2"`, `action`} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err, part)
		}
	}
}

func TestNewRegistry_AllowsDeclaredActionOverride(t *testing.T) {
	t.Parallel()
	original := action.New("task.run", func(_ context.Context, in string) (string, error) {
		return "original:" + in, nil
	}).Build()
	replacement := action.New("task.run", func(_ context.Context, in string) (string, error) {
		return "replacement:" + in, nil
	}).Build()
	reg, err := action.NewRegistry(
		action.Library{Name: "base", Actions: []action.AnyAction{original}},
		action.Library{Name: "test_override", Actions: []action.AnyAction{replacement}, Overrides: []string{"task.run"}},
	)
	if err != nil {
		t.Fatalf("declared action override: %v", err)
	}
	got, ok := reg.Get("task.run")
	if !ok || got != replacement {
		t.Fatal("registry did not select the explicitly overridden action")
	}
	if reg.Len() != 1 {
		t.Fatalf("override created a duplicate action entry: Len() = %d", reg.Len())
	}
	actions := reg.Actions()
	if len(actions) != 1 || actions[0] != replacement {
		t.Fatalf("Actions() is inconsistent with Get(): %#v", actions)
	}
}

func TestNewRegistry_RejectsUnmatchedActionOverride(t *testing.T) {
	t.Parallel()

	replacement := action.New("task.run", func(_ context.Context, in string) (string, error) {
		return "replacement:" + in, nil
	}).Build()
	tests := []struct {
		name      string
		libraries []action.Library
		want      string
	}{
		{
			name: "target is not registered",
			libraries: []action.Library{{
				Name: "test_override", Actions: []action.AnyAction{replacement}, Overrides: []string{"task.run"},
			}},
			want: `overrides action "task.run" without a previously registered target`,
		},
		{
			name: "replacement action is not declared",
			libraries: []action.Library{{
				Name: "test_override", Overrides: []string{"task.run"},
			}},
			want: `declares override for action "task.run" but does not declare that action`,
		},
		{
			name: "target is not an action",
			libraries: []action.Library{
				{Name: "base", Sources: []action.AnyStreamAction{intSource("task.run", 1)}},
				{Name: "test_override", Actions: []action.AnyAction{replacement}, Overrides: []string{"task.run"}},
			},
			want: `kind action owner "test_override" conflicts with kind source owner "base"`,
		},
		{
			name: "override is declared more than once",
			libraries: []action.Library{
				{Name: "base", Actions: []action.AnyAction{action.New("task.run", dummyHandler).Build()}},
				{Name: "test_override", Actions: []action.AnyAction{replacement}, Overrides: []string{"task.run", "task.run"}},
			},
			want: `declares override for action "task.run" more than once`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reg, err := action.NewRegistry(test.libraries...)
			if err == nil {
				t.Fatal("expected an error for an unmatched override")
			}
			if reg != nil {
				t.Fatal("invalid override returned a partially built registry")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error %q does not contain %q", err, test.want)
			}
		})
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
	for range b.N {
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
	for range b.N {
		_, _ = reg.Get("b")
	}
}

func BenchmarkRegistry_Actions(b *testing.B) {
	act := action.New("bench.op", dummyHandler).Build()
	reg := action.MustNewRegistry(action.Library{Name: "bench", Actions: []action.AnyAction{act}})

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = reg.Actions()
	}
}

func dummyHandler(_ context.Context, req any) (any, error) {
	return req, nil
}
