package dag_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestDAG_ToMermaid_Deterministic(t *testing.T) {
	t.Parallel()

	dummy := action.New("dummy", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "ok", nil
	}).Build()

	cdag, err := dag.New("pipeline").
		AddNode("user.fetch", "user", dummy).
		AddNode("orders.fetch", "orders", dummy).
		AddNode("ai.synthesize", "result", dummy).
		AddEdge("user.fetch", "ai.synthesize").
		AddEdge("orders.fetch", "ai.synthesize").
		Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	mermaid1 := cdag.ToMermaid()
	mermaid2 := cdag.ToMermaid()

	if mermaid1 != mermaid2 {
		t.Fatalf("ToMermaid must be 100%% deterministic across calls:\nRun 1:\n%s\nRun 2:\n%s", mermaid1, mermaid2)
	}

	expectedPrefix := "flowchart LR\n"
	if !strings.HasPrefix(mermaid1, expectedPrefix) {
		t.Errorf("expected header %q, got:\n%s", expectedPrefix, mermaid1)
	}

	expectedNodes := []string{
		`id_ai_synthesize["ai.synthesize"]`,
		`id_orders_fetch["orders.fetch"]`,
		`id_user_fetch["user.fetch"]`,
	}
	for _, expectedNode := range expectedNodes {
		if !strings.Contains(mermaid1, expectedNode) {
			t.Errorf("expected node declaration %q in:\n%s", expectedNode, mermaid1)
		}
	}

	expectedEdges := []string{
		"id_orders_fetch --> id_ai_synthesize",
		"id_user_fetch --> id_ai_synthesize",
	}
	for _, expectedEdge := range expectedEdges {
		if !strings.Contains(mermaid1, expectedEdge) {
			t.Errorf("expected edge %q in:\n%s", expectedEdge, mermaid1)
		}
	}
}

func TestDAG_ToMermaid_NilAndEmpty(t *testing.T) {
	t.Parallel()

	var nilDAG *dag.DAG
	if got := nilDAG.ToMermaid(); got != "" {
		t.Errorf("nil DAG must return empty string, got %q", got)
	}
}

func TestDAG_ToMermaid_LabelEscaping(t *testing.T) {
	t.Parallel()

	dummy := action.New("dummy", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "ok", nil
	}).Build()

	cdag, err := dag.New("special_chars").
		AddNode(`node "A"`, "out", dummy).
		Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	out := cdag.ToMermaid()
	if !strings.Contains(out, `["node #quot;A#quot;"]`) {
		t.Fatalf("expected escaped quotes in label, got:\n%s", out)
	}
}

func BenchmarkDAG_ToMermaid(b *testing.B) {
	dummy := action.New("dummy", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "ok", nil
	}).Build()

	cdag, _ := dag.New("bench_dag").
		AddNode("node1", "out1", dummy).
		AddNode("node2", "out2", dummy).
		AddNode("node3", "out3", dummy).
		AddNode("node4", "out4", dummy).
		AddEdge("node1", "node3").
		AddEdge("node2", "node3").
		AddEdge("node3", "node4").
		Compile()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res := cdag.ToMermaid()
		if len(res) == 0 {
			b.Fatal("unexpected empty result")
		}
	}
}
