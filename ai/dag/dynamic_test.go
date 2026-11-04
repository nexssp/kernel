package dag_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestDynamicResumeSkipsCompletedNode(t *testing.T) {
	calls := 0
	act := action.New("step", func(context.Context, dag.ReadState) (string, error) {
		calls++
		return "rerun", nil
	}).Build()
	graph, err := dag.New(t.Name()).
		AddNode("step", "step", act).
		AddConditionalEdge("step", func(context.Context, dag.ReadState) (string, error) {
			return "end", nil
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
	if calls != 0 {
		t.Fatalf("node calls = %d, want 0 for completed node", calls)
	}
	if value, ok := state.Get(dag.OutputKey("step")); !ok || value != "saved" {
		t.Fatalf("resumed output = (%v, %t), want (saved, true)", value, ok)
	}
}

func TestDynamicResumePreservesNilOutput(t *testing.T) {
	calls := 0
	act := action.New("step", func(context.Context, dag.ReadState) (any, error) {
		calls++
		return nil, nil
	}).Build()
	graph, err := dag.New(t.Name()).
		AddNode("step", "step", act).
		AddConditionalEdge("step", func(context.Context, dag.ReadState) (string, error) {
			return "end", nil
		}).
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	first, err := graph.Execute(context.Background(), dag.ReadState{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	if value, ok := first.Get(dag.OutputKey("step")); !ok || value != nil {
		t.Fatalf("first output = (%v, %t), want (nil, true)", value, ok)
	}

	resumed, err := graph.Execute(context.Background(), first.AsRead())
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Release()
	if calls != 1 {
		t.Fatalf("node calls after resume = %d, want 1", calls)
	}
}

func TestLayeredResumePreservesNilOutput(t *testing.T) {
	calls := 0
	act := action.New("step", func(context.Context, dag.ReadState) (any, error) {
		calls++
		return nil, nil
	}).Build()
	graph, err := dag.New(t.Name()).AddNode("step", "step", act).Compile()
	if err != nil {
		t.Fatal(err)
	}

	first, err := graph.Execute(context.Background(), dag.ReadState{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	if value, ok := first.Get(dag.OutputKey("step")); !ok || value != nil {
		t.Fatalf("first output = (%v, %t), want (nil, true)", value, ok)
	}

	resumed, err := graph.Execute(context.Background(), first.AsRead())
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Release()
	if calls != 1 {
		t.Fatalf("node calls after resume = %d, want 1", calls)
	}
}

func TestCompileRejectsMixedStaticAndConditionalEdges(t *testing.T) {
	router := func(context.Context, dag.ReadState) (string, error) { return "end", nil }
	newBuilder := func(name string) *dag.Builder {
		act := action.New("noop", func(context.Context, dag.ReadState) (struct{}, error) {
			return struct{}{}, nil
		}).Build()
		return dag.New(name).
			AddNode("a", "a", act).
			AddNode("b", "b", act)
	}

	staticFirst := newBuilder("static-first").AddEdge("a", "b").AddConditionalEdge("a", router)
	if _, err := staticFirst.Compile(); err == nil {
		t.Fatal("Compile() accepted static edges followed by a conditional edge")
	}

	conditionalFirst := newBuilder("conditional-first").AddConditionalEdge("a", router).AddEdge("a", "b")
	if _, err := conditionalFirst.Compile(); err == nil {
		t.Fatal("Compile() accepted a conditional edge followed by static edges")
	}
}
