package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestMetaMatchesNode(t *testing.T) {
	t.Parallel()

	monolith := action.NodeFilterPolicy{ActiveNode: ""}

	workerDefault := action.NodeFilterPolicy{
		ActiveNode:  "worker",
		DefaultNode: "worker",
	}

	tests := []struct {
		name   string
		meta   *action.Meta
		policy action.NodeFilterPolicy
		want   bool
	}{
		{
			name:   "monolith includes any non-nil action",
			meta:   &action.Meta{Name: "x", Node: "ai"},
			policy: monolith,
			want:   true,
		},
		{
			name:   "nil meta is never local",
			meta:   nil,
			policy: workerDefault,
			want:   false,
		},
		{
			name:   "system action runs on every node",
			meta:   &action.Meta{Name: "health", Scope: action.ScopeSystem, Node: "ai"},
			policy: workerDefault,
			want:   true,
		},
		{
			name:   "exact node match",
			meta:   &action.Meta{Name: "task", Node: "worker"},
			policy: workerDefault,
			want:   true,
		},
		{
			name:   "different node is filtered",
			meta:   &action.Meta{Name: "billing", Node: "billing"},
			policy: workerDefault,
			want:   false,
		},
		{
			name:   "untagged action runs on default node",
			meta:   &action.Meta{Name: "shared"},
			policy: workerDefault,
			want:   true,
		},
		{
			name: "untagged action is filtered when default is elsewhere",
			meta: &action.Meta{Name: "shared"},
			policy: action.NodeFilterPolicy{
				ActiveNode:  "worker",
				DefaultNode: "gateway",
			},
			want: false,
		},
		{
			name: "untagged action allowed everywhere",
			meta: &action.Meta{Name: "shared"},
			policy: action.NodeFilterPolicy{
				ActiveNode:              "worker",
				AllowUntaggedEverywhere: true,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.meta.MatchesNode(tt.policy)
			if got != tt.want {
				t.Fatalf("MatchesNode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilterByNode(t *testing.T) {
	t.Parallel()

	buildAction := func(name string) action.AnyAction {
		return action.New(name, func(_ context.Context, _ struct{}) (string, error) {
			return name, nil
		}).Build()
	}

	systemAct := buildAction("health")
	// A system action after creation still needs Scope set.
	systemAct.Describe().Scope = action.ScopeSystem // not ideal; use builder below instead

	// Exact typed builder is clearer:
	builderSystem := action.New("health", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).System().Build()

	workerAct := action.New("worker.job", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).Node("worker").Build()

	aiAct := action.New("ai.job", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).Node("ai").Build()

	untagged := action.New("shared.job", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).Build()

	all := []action.AnyAction{builderSystem, workerAct, aiAct, untagged}

	policy := action.NodeFilterPolicy{
		ActiveNode:  "worker",
		DefaultNode: "worker",
	}

	got := action.FilterByNode(all, policy)

	names := make([]string, len(got))
	for i, act := range got {
		names[i] = act.Describe().Name
	}

	if len(names) != 3 {
		t.Fatalf("FilterByNode() returned %d actions: %v", len(names), names)
	}

	// expect: health, worker.job, shared.job
	// ai.job must be absent
}
