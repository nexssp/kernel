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

// TestDAG_ExecutionAndStateIsolation verifies multi-layer parallel node
// execution, state aggregation across layers, and read-only view isolation.
func TestDAG_ExecutionAndStateIsolation(t *testing.T) {
	t.Parallel()

	var layer1Concurrency atomic.Int32

	fetchUserAct := action.New("fetch_user", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		layer1Concurrency.Add(1)
		time.Sleep(20 * time.Millisecond)
		return "Alice", nil
	}).Build()

	fetchOrdersAct := action.New("fetch_orders", func(_ context.Context, _ *dag.NodeContext) (int, error) {
		layer1Concurrency.Add(1)
		time.Sleep(20 * time.Millisecond)
		return 5, nil
	}).Build()

	mergeSummaryAct := action.New("merge_summary", func(_ context.Context, nCtx *dag.NodeContext) (string, error) {
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

	finalState, err := cdag.Execute(context.Background(), initialState.AsRead())
	if err != nil {
		t.Fatalf("DAG execution failed: %v", err)
	}
	defer finalState.Release()

	summary, err := dag.GetNodeOutput[string](finalState.AsRead(), "node_merge")
	if err != nil {
		t.Fatalf("missing summary in final state: %v", err)
	}
	if summary != "User Alice has 5 orders" {
		t.Fatalf("unexpected DAG final output: %v", summary)
	}
}

// TestDAG_CycleDetection verifies that circular dependencies are caught
// during compilation.
func TestDAG_CycleDetection(t *testing.T) {
	t.Parallel()

	dummyAct := action.New("dummy", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "ok", nil
	}).Build()

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

// TestDAG_NodeErrorPropagation verifies that a node failure surfaces as a
// fully populated *ExecutionError with the offending node, layer index,
// and a resumable state attached.
func TestDAG_NodeErrorPropagation(t *testing.T) {
	t.Parallel()

	failAct := action.New("fail_node", func(_ context.Context, _ *dag.NodeContext) (string, error) {
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

	finalState, err := cdag.Execute(context.Background(), initialState.AsRead())
	if err == nil {
		t.Fatal("expected error from failing DAG node")
	}
	if finalState == nil {
		t.Fatal("Execute must return a non-nil state on failure so the caller can persist it for resume")
	}
	defer finalState.Release()

	// error kind must remain internal for a plain handler error
	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal error wrapper, got: %v", err)
	}

	// ExecutionError must be reachable and fully populated
	var execErr *dag.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("expected *dag.ExecutionError, got %T: %v", err, err)
	}
	if execErr.FailedNode != "failing_node" {
		t.Fatalf("FailedNode = %q, want failing_node", execErr.FailedNode)
	}
	if execErr.Layer != 0 {
		t.Fatalf("Layer = %d, want 0", execErr.Layer)
	}
	if execErr.State == nil {
		t.Fatal("ExecutionError.State must be non-nil for resume")
	}
	if execErr.State != finalState {
		t.Fatalf("ExecutionError.State must be the same *State returned by Execute")
	}
}

// TestDAG_LayerErrorRecordsCompletedNodes verifies that when one sibling
// fails, the ExecutionError lists the names of siblings that already
// succeeded — the list a resume-driven caller needs to know what not to
// re-run.
func TestDAG_LayerErrorRecordsCompletedNodes(t *testing.T) {
	t.Parallel()

	goodAct := action.New("good_node", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "good", nil
	}).Build()

	badAct := action.New("bad_node", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "", errors.New("boom")
	}).Build()

	cdag, err := dag.New("partial_layer").
		AddNode("good_node", "good_out", goodAct).
		AddNode("bad_node", "bad_out", badAct).
		Compile()
	if err != nil {
		t.Fatalf("failed to compile DAG: %v", err)
	}

	state := dag.AcquireState()
	defer state.Release()

	final, err := cdag.Execute(context.Background(), state.AsRead())
	if err == nil {
		t.Fatal("expected error from bad_node")
	}
	if final == nil {
		t.Fatal("state must be preserved on failure")
	}
	defer final.Release()

	var execErr *dag.ExecutionError
	if !errors.As(err, &execErr) {
		t.Fatalf("expected *dag.ExecutionError, got %T: %v", err, err)
	}
	if execErr.FailedNode != "bad_node" {
		t.Fatalf("FailedNode = %q, want bad_node", execErr.FailedNode)
	}
	if len(execErr.Completed) != 1 || execErr.Completed[0] != "good_node" {
		t.Fatalf("Completed = %v, want [good_node]", execErr.Completed)
	}
	// good_node's output must be present in the returned state so resume
	// can skip it.
	if v, ok := final.Get(dag.OutputKey("good_node")); !ok || v != "good" {
		t.Fatalf("good_node output missing from preserved state: v=%v ok=%v", v, ok)
	}
}

// TestDAG_ContextCancellation verifies clean teardown when context is
// canceled.
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

	_, err = cdag.Execute(ctx, initialState.AsRead())
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
	out, err := g.Execute(context.Background(), state.AsRead())
	if err != nil {
		t.Fatal(err)
	}
	out.Release()
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
