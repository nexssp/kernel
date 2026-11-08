package dag_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestDynamicExecutionAllowsNodeRevisits(t *testing.T) {
	var draftCalls, reviewCalls int
	draft := action.New("draft", func(context.Context, dag.ReadState) (int, error) {
		draftCalls++
		return draftCalls, nil
	}).Build()
	review := action.New("review", func(context.Context, dag.ReadState) (int, error) {
		reviewCalls++
		return reviewCalls, nil
	}).Build()

	graph, err := dag.New(t.Name()).
		AddNode("draft", "draft", draft).
		AddNode("review", "review", review).
		AddConditionalEdge("draft", func(_ context.Context, state dag.ReadState) (string, error) {
			if revision, _ := state.Get(dag.OutputKey("draft")); revision == 2 {
				return "end", nil
			}
			return "review", nil
		}).
		AddConditionalEdge("review", func(context.Context, dag.ReadState) (string, error) {
			return "draft", nil
		}).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	state, err := graph.Execute(context.Background(), dag.ReadState{})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Release()

	if draftCalls != 2 || reviewCalls != 1 {
		t.Fatalf("action calls: draft=%d review=%d, want 2 and 1", draftCalls, reviewCalls)
	}
	if got, ok := state.Get(dag.OutputKey("draft")); !ok || got != 2 {
		t.Fatalf("draft output = (%v, %t), want (2, true)", got, ok)
	}
	if got, ok := state.Get(dag.OutputKey("review")); !ok || got != 1 {
		t.Fatalf("review output = (%v, %t), want (1, true)", got, ok)
	}
}

func TestDynamicExecutionKeepsInitialNodeCompletedOnRevisit(t *testing.T) {
	var stepCalls, reviewCalls, routeCalls int
	step := action.New("step", func(context.Context, dag.ReadState) (string, error) {
		stepCalls++
		return "rerun", nil
	}).Build()
	review := action.New("review", func(context.Context, dag.ReadState) (string, error) {
		reviewCalls++
		return "reviewed", nil
	}).Build()

	graph, err := dag.New(t.Name()).
		AddNode("step", "step", step).
		AddNode("review", "review", review).
		AddConditionalEdge("step", func(context.Context, dag.ReadState) (string, error) {
			routeCalls++
			if routeCalls == 1 {
				return "review", nil
			}
			return "end", nil
		}).
		AddConditionalEdge("review", func(context.Context, dag.ReadState) (string, error) {
			return "step", nil
		}).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	initial := dag.AcquireState()
	defer initial.Release()
	initial.Set(dag.OutputKey("step"), "saved")

	state, err := graph.Execute(context.Background(), initial.AsRead())
	if err != nil {
		t.Fatal(err)
	}
	defer state.Release()

	if stepCalls != 0 || reviewCalls != 1 || routeCalls != 2 {
		t.Fatalf("calls: step=%d review=%d routes=%d, want 0, 1, 2", stepCalls, reviewCalls, routeCalls)
	}
	if got, ok := state.Get(dag.OutputKey("step")); !ok || got != "saved" {
		t.Fatalf("persisted step output = (%v, %t), want (saved, true)", got, ok)
	}
}

func TestDynamicExecutionBoundsRepeatingNodeVisits(t *testing.T) {
	calls := 0
	loop := action.New("loop", func(context.Context, dag.ReadState) (int, error) {
		calls++
		return calls, nil
	}).Build()
	graph, err := dag.New(t.Name()).
		AddNode("loop", "loop", loop).
		AddConditionalEdge("loop", func(context.Context, dag.ReadState) (string, error) {
			return "loop", nil
		}).
		WithMaxSteps(3).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	state, err := graph.Execute(context.Background(), dag.ReadState{})
	if state == nil {
		t.Fatal("Execute returned nil state for a max-step error")
	}
	defer state.Release()
	if err == nil || !strings.Contains(err.Error(), "exceeded 3 steps") {
		t.Fatalf("Execute() error = %v, want max-step timeout", err)
	}
	if calls != 3 {
		t.Fatalf("node calls = %d, want 3", calls)
	}
	if got, ok := state.Get(dag.OutputKey("loop")); !ok || got != 3 {
		t.Fatalf("last loop output = (%v, %t), want (3, true)", got, ok)
	}
}
