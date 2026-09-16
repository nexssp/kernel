package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestNewRegistry_DeterminismAndAliases(t *testing.T) {
	a1 := action.New("b.second", func(ctx context.Context, _ any) (any, error) { return nil, nil }).Build()
	a2 := action.New("a.first", func(ctx context.Context, _ any) (any, error) { return nil, nil }).Build()

	lib := action.Library{
		Name:    "core",
		Actions: []action.AnyAction{a1, a2},
		Aliases: []action.Alias{
			{Canonical: "a.first", Short: []string{"first", "f"}},
		},
	}

	reg, err := action.NewRegistry(lib)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 1. Determinizm sortowania kanonicznych akcji
	actions := reg.Actions()
	if len(actions) != 2 {
		t.Fatalf("expected 2 actions, got %d", len(actions))
	}
	if actions[0].Describe().Name != "a.first" || actions[1].Describe().Name != "b.second" {
		t.Errorf("actions not sorted properly: %v, %v", actions[0].Describe().Name, actions[1].Describe().Name)
	}

	// 2. Rozwiązywanie aliasów w Get()
	if act, ok := reg.Get("first"); !ok || act != a2 {
		t.Errorf("expected alias 'first' to resolve to a2")
	}
	if act, ok := reg.Get("f"); !ok || act != a2 {
		t.Errorf("expected alias 'f' to resolve to a2")
	}
}

func TestNewRegistry_CollisionRequiresOverride(t *testing.T) {
	actOrig := action.New("task.run", func(ctx context.Context, _ any) (any, error) { return "orig", nil }).Build()
	actNew := action.New("task.run", func(ctx context.Context, _ any) (any, error) { return "new", nil }).Build()

	lib1 := action.Library{Name: "base", Actions: []action.AnyAction{actOrig}}
	lib2 := action.Library{Name: "custom", Actions: []action.AnyAction{actNew}}

	// Bez Overrides powinno rzucić błąd
	if _, err := action.NewRegistry(lib1, lib2); err == nil {
		t.Fatalf("expected collision error, got nil")
	}

	// Z Overrides powinno przejść i wybrać actNew
	lib2.Overrides = []string{"task.run"}
	reg, err := action.NewRegistry(lib1, lib2)
	if err != nil {
		t.Fatalf("unexpected error with override: %v", err)
	}
	if got, _ := reg.Get("task.run"); got != actNew {
		t.Errorf("expected overridden action actNew to win")
	}
}

func TestNewRegistry_SharedActionAccumulatesHooks(t *testing.T) {
	sharedAct := action.New("shared.op", func(ctx context.Context, _ any) (any, error) { return nil, nil }).Build()

	var count int
	hook := action.AnyHook{
		Before: func(ctx context.Context, req any, meta *action.Meta) (context.Context, error) {
			count++
			return ctx, nil
		},
	}

	lib := action.Library{
		Name:    "with_hook",
		Actions: []action.AnyAction{sharedAct},
		Hooks:   []action.AnyHook{hook},
	}

	_, _ = action.NewRegistry(lib)
	_, _ = action.NewRegistry(lib)

	// Dokumentujemy zachowanie: AddAnyHook mutuje obiekt akcji
	if len(sharedAct.GetAnyHooks()) != 2 {
		t.Errorf("expected 2 hooks accumulated on shared action, got %d", len(sharedAct.GetAnyHooks()))
	}
}
