package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

// TestRegistryActionReuseAcrossRegistries proves that the same
// BuiltAction can be registered in any number of registries. Because
// NewRegistry never mutates actions, this is trivially true — the test
// exists to lock the behavior in place.
func TestRegistryActionReuseAcrossRegistries(t *testing.T) {
	act := action.New("user.get", func(_ context.Context, id string) (string, error) {
		return "user:" + id, nil
	}).Build()

	libA := action.Library{Name: "libA", Actions: []action.AnyAction{act}}
	libB := action.Library{Name: "libB", Actions: []action.AnyAction{act}}

	regA, err := action.NewRegistry(libA)
	if err != nil {
		t.Fatalf("first registry: %v", err)
	}
	regB, err := action.NewRegistry(libB)
	if err != nil {
		t.Fatalf("second registry: %v", err)
	}
	if _, err := action.NewRegistry(libA); err != nil {
		t.Fatalf("third registry: %v", err)
	}

	for name, reg := range map[string]*action.Registry{"A": regA, "B": regB} {
		got, ok := reg.Get("user.get")
		if !ok {
			t.Fatalf("registry %s: missing user.get", name)
		}
		res, err := got.DoAny(context.Background(), "42")
		if err != nil {
			t.Fatalf("registry %s call: %v", name, err)
		}
		if res != "user:42" {
			t.Fatalf("registry %s: want user:42, got %v", name, res)
		}
	}
}
