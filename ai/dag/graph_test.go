package dag_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
	"github.com/nexssp/kernel/xerr"
)

// TestDAG_ExecutionAndStateIsolation verifies multi-layer parallel node execution,
// state aggregation across layers, and zero-lock state isolation.
func TestDAG_ExecutionAndStateIsolation(t *testing.T) {
	t.Parallel()

	var layer1Concurrency atomic.Int32

	// Layer 1: Parallel User & Order fetching
	fetchUserAct := action.New("fetch_user", func(ctx context.Context, nCtx *dag.NodeContext) (string, error) {
		layer1Concurrency.Add(1)
		time.Sleep(20 * time.Millisecond)
		return "Alice", nil
	}).Build()

	fetchOrdersAct := action.New("fetch_orders", func(ctx context.Context, nCtx *dag.NodeContext) (int, error) {
		layer1Concurrency.Add(1)
		time.Sleep(20 * time.Millisecond)
		return 5, nil
	}).Build()

	// Layer 2: Dependent Summary Aggregation
	mergeSummaryAct := action.New("merge_summary", func(ctx context.Context, nCtx *dag.NodeContext) (string, error) {
		if nCtx == nil || nCtx.Input == nil {
			return "", errors.New("node context or input state is nil")
		}

		userName, err := dag.GetNodeOutput[string](nCtx.Input, "node_user")
		if err != nil {
			return "", err
		}
		orderCount, err := dag.GetNodeOutput[int](nCtx.Input, "node_orders")
		if err != nil {
			return "", err
		}

		if layer1Concurrency.Load() < 2 {
			t.Errorf("expected Layer 1 nodes to execute concurrently")
		}

		return fmt.Sprintf("User %s has %d orders", userName, orderCount), nil
	}).Build()

	cdag, err := dag.New("test_user_summary_dag").
		AddNode("node_user", "user_name", fetchUserAct).
		AddNode("node_orders", "order_count", fetchOrdersAct).
		AddNode("node_merge", "summary", mergeSummaryAct).
		AddEdge("node_user", "node_merge").
		AddEdge("node_orders", "node_merge").
		Compile()
	if err != nil {
		t.Fatalf("failed to compile DAG: %v", err)
	}

	initialState := dag.AcquireState()
	defer initialState.Release()

	finalState, err := cdag.Execute(context.Background(), initialState)
	if err != nil {
		t.Fatalf("DAG execution failed: %v", err)
	}
	defer finalState.Release()

	summary, err := dag.GetNodeOutput[string](finalState, "node_merge")
	if err != nil {
		t.Fatalf("missing summary in final state: %v", err)
	}

	if summary != "User Alice has 5 orders" {
		t.Fatalf("unexpected DAG final output: %v", summary)
	}
}

// TestDAG_CycleDetection verifies that circular dependencies are caught during compilation.
func TestDAG_CycleDetection(t *testing.T) {
	t.Parallel()

	dummyAct := action.New("dummy", func(ctx context.Context, _ *dag.NodeContext) (string, error) {
		return "ok", nil
	}).Build()

	// Circular dependency: A -> B -> C -> A
	_, err := dag.New("cycle_dag").
		AddNode("A", "out_a", dummyAct).
		AddNode("B", "out_b", dummyAct).
		AddNode("C", "out_c", dummyAct).
		AddEdge("A", "B").
		AddEdge("B", "C").
		AddEdge("C", "A").
		Compile()

	if err == nil || !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("expected cycle detection error during compilation, got: %v", err)
	}
}

// TestDAG_NodeErrorPropagation verifies that node errors abort execution cleanly.
func TestDAG_NodeErrorPropagation(t *testing.T) {
	t.Parallel()

	failAct := action.New("fail_node", func(ctx context.Context, nCtx *dag.NodeContext) (string, error) {
		return "", errors.New("db connection lost")
	}).Build()

	cdag, err := dag.New("error_dag").
		AddNode("failing_node", "out", failAct).
		Compile()
	if err != nil {
		t.Fatalf("failed to compile DAG: %v", err)
	}

	initialState := dag.AcquireState()
	defer initialState.Release()

	_, err = cdag.Execute(context.Background(), initialState)
	if err == nil {
		t.Fatal("expected error from failing DAG node")
	}

	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal error wrapper, got: %v", err)
	}
}

// TestDAG_ContextCancellation verifies clean teardown when context is canceled.
func TestDAG_ContextCancellation(t *testing.T) {
	t.Parallel()

	slowAct := action.New("slow_node", func(ctx context.Context, _ *dag.NodeContext) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(1 * time.Second):
			return "done", nil
		}
	}).Build()

	cdag, err := dag.New("cancel_dag").
		AddNode("slow", "out", slowAct).
		Compile()
	if err != nil {
		t.Fatalf("failed to compile DAG: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	initialState := dag.AcquireState()
	defer initialState.Release()

	_, err = cdag.Execute(ctx, initialState)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
}

func TestDAG_DeterministicLayerOrder(t *testing.T) {
	dummy := action.New("dummy", func(context.Context, *dag.NodeContext) (string, error) { return "ok", nil }).Build()
	g, err := dag.New("ordered").
		AddNode("z", "z", dummy).
		AddNode("a", "a", dummy).
		AddNode("m", "m", dummy).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := dag.AcquireState()
	defer state.Release()
	out, err := g.Execute(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
}

func TestDAG_StateDataIsSnapshot(t *testing.T) {
	state := dag.AcquireState()
	defer state.Release()
	state.Set("key", "before")
	data := state.Data()
	data["key"] = "after"
	data["new"] = true
	value, _ := state.Get("key")
	if value != "before" {
		t.Fatalf("Data mutated internal state: %v", value)
	}
	if _, ok := state.Get("new"); ok {
		t.Fatal("Data exposed internal map")
	}
}

func TestDAG_InvalidBuilderInputReturnsCompileError(t *testing.T) {
	builder := dag.New("invalid").AddNode("", "", nil)
	if _, err := builder.Compile(); err == nil {
		t.Fatal("expected invalid node error")
	}
}

func TestDAG_DuplicateEdgeReturnsCompileError(t *testing.T) {
	dummy := action.New("dummy", func(context.Context, *dag.NodeContext) (string, error) { return "ok", nil }).Build()
	builder := dag.New("duplicate").AddNode("a", "a", dummy).AddNode("b", "b", dummy).AddEdge("a", "b").AddEdge("a", "b")
	if _, err := builder.Compile(); err == nil {
		t.Fatal("expected duplicate edge error")
	}
}
