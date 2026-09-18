package dag_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestDAG_AllNodesSkippedWithCancelledCtx(t *testing.T) {
	dummy := action.New("dummy", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "ok", nil
	}).Build()

	g, _ := dag.New("skip_all").AddNode("a", "a", dummy).AddNode("b", "b", dummy).AddEdge("a", "b").Compile()

	state := dag.AcquireState()
	defer state.Release()
	state.Set("tasks.a.output", "ok")
	state.Set("tasks.b.output", "ok")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	final, err := g.Execute(ctx, state.AsRead())
	if err != nil {
		t.Fatalf("expected nil error since all nodes are skipped, got %v", err)
	}
	defer final.Release()

	if v, _ := final.Get("tasks.a.output"); v != "ok" {
		t.Fatalf("state lost")
	}
}
