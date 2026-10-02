package action_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestNewRegistry_RejectsMalformedAliases(t *testing.T) {
	t.Parallel()
	canonical := action.New("target.run", func(_ context.Context, in any) (any, error) {
		return in, nil
	}).Build()

	tests := []struct {
		name  string
		alias action.Alias
		want  string
	}{
		{name: "empty canonical", alias: action.Alias{Short: []string{"run"}}, want: "malformed alias"},
		{name: "empty short list", alias: action.Alias{Canonical: "target.run"}, want: "at least one short name"},
		{name: "empty short name", alias: action.Alias{Canonical: "target.run", Short: []string{""}}, want: "malformed alias name"},
		{name: "self alias", alias: action.Alias{Canonical: "target.run", Short: []string{"target.run"}}, want: "repeats canonical"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reg, err := action.NewRegistry(action.Library{
				Name:    "broken",
				Actions: []action.AnyAction{canonical},
				Aliases: []action.Alias{test.alias},
			})
			if err == nil {
				t.Fatal("expected malformed alias error")
			}
			if reg != nil {
				t.Fatal("malformed alias returned a partial registry")
			}
			if !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), `"broken"`) {
				t.Fatalf("error %q does not identify the malformed alias and owner", err)
			}
		})
	}
}

func TestNewRegistry_RejectsAliasCanonicalCollisionAcrossKinds(t *testing.T) {
	t.Parallel()
	canonical := action.New("target.run", func(_ context.Context, in any) (any, error) {
		return in, nil
	}).Build()
	_, err := action.NewRegistry(
		action.Library{Name: "source_owner", Sources: []action.AnyStreamAction{intSource("source.name", 1)}},
		action.Library{
			Name:    "alias_owner",
			Actions: []action.AnyAction{canonical},
			Aliases: []action.Alias{{Canonical: "target.run", Short: []string{"source.name"}}},
		},
	)
	if err == nil {
		t.Fatal("expected alias-vs-canonical collision")
	}
	for _, part := range []string{`name "source.name"`, "kind alias", `owner "alias_owner"`, "kind source", `owner "source_owner"`} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err, part)
		}
	}
}

func TestNewRegistry_RejectsCanonicalCrossKindCollisions(t *testing.T) {
	t.Parallel()
	actionItem := func(name string) action.AnyAction {
		return action.New(name, func(_ context.Context, in any) (any, error) { return in, nil }).Build()
	}
	tests := []struct {
		name   string
		libs   []action.Library
		kinds  []string
		owners []string
	}{
		{
			name: "action and source",
			libs: []action.Library{
				{Name: "action_owner", Actions: []action.AnyAction{actionItem("shared.name")}},
				{Name: "source_owner", Sources: []action.AnyStreamAction{intSource("shared.name", 1)}},
			},
			kinds: []string{"action", "source"}, owners: []string{"action_owner", "source_owner"},
		},
		{
			name: "action and operator",
			libs: []action.Library{
				{Name: "action_owner", Actions: []action.AnyAction{actionItem("shared.name")}},
				{Name: "operator_owner", Operators: []action.NamedOperator{noopOperator[int]("shared.name")}},
			},
			kinds: []string{"action", "operator"}, owners: []string{"action_owner", "operator_owner"},
		},
		{
			name: "source and operator",
			libs: []action.Library{
				{Name: "source_owner", Sources: []action.AnyStreamAction{intSource("shared.name", 1)}},
				{Name: "operator_owner", Operators: []action.NamedOperator{noopOperator[int]("shared.name")}},
			},
			kinds: []string{"source", "operator"}, owners: []string{"source_owner", "operator_owner"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reg, err := action.NewRegistry(test.libs...)
			if err == nil {
				t.Fatal("expected cross-kind collision")
			}
			if reg != nil {
				t.Fatal("cross-kind collision returned a partial registry")
			}
			for _, part := range append([]string{`name "shared.name"`}, append(test.kinds, test.owners...)...) {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q does not contain %q", err, part)
				}
			}
		})
	}
}
