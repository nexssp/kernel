package dag_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestNamespacedOutputsPreventParallelCollision(t *testing.T) {
	left := action.New("left", func(context.Context, *dag.NodeContext) (string, error) { return "left", nil }).Build()
	right := action.New("right", func(context.Context, *dag.NodeContext) (string, error) { return "right", nil }).Build()
	graph, err := dag.New("fanout").
		AddNode("left", "report", left).
		AddNode("right", "report", right).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := dag.AcquireState()
	defer state.Release()
	out, err := graph.Execute(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got, err := dag.GetNodeOutput[string](out, "left"); err != nil || got != "left" {
		t.Fatalf("left=%q err=%v", got, err)
	}
	if got, err := dag.GetNodeOutput[string](out, "right"); err != nil || got != "right" {
		t.Fatalf("right=%q err=%v", got, err)
	}
	if _, ok := out.Get("report"); ok {
		t.Fatal("un-namespaced report key was written")
	}
}

func TestTypedOutputRejectsMismatch(t *testing.T) {
	state := dag.AcquireState()
	defer state.Release()
	state.Set(dag.OutputKey("worker"), "not-a-report")
	_, err := dag.GetNodeOutput[struct{ Score int }](state, "worker")
	if err == nil || !strings.Contains(err.Error(), "expected") {
		t.Fatalf("expected typed mismatch, got %v", err)
	}
}
