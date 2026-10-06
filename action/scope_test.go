package action_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

func TestScope_BasicAccessors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctx = action.WithExecutionID(ctx, "exec-123")
	ctx = action.WithTraceContext(ctx, "trace-abc", "span-xyz")

	if got := action.ExecutionIDFrom(ctx); got != "exec-123" {
		t.Fatalf("ExecutionIDFrom = %q, want 'exec-123'", got)
	}
	if got := action.TraceIDFrom(ctx); got != "trace-abc" {
		t.Fatalf("TraceIDFrom = %q, want 'trace-abc'", got)
	}
	if got := action.SpanIDFrom(ctx); got != "span-xyz" {
		t.Fatalf("SpanIDFrom = %q, want 'span-xyz'", got)
	}
}

func TestWithExecutionID_ChildIsolation(t *testing.T) {
	t.Parallel()

	parentCtx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	childCtxA := xctx.WithExecutionID(parentCtx, "exec-task-A")
	childCtxB := xctx.WithExecutionID(parentCtx, "exec-task-B")

	gotA := xctx.ExecutionIDFrom(childCtxA)
	gotB := xctx.ExecutionIDFrom(childCtxB)

	if gotA != "exec-task-A" {
		t.Fatalf("scope isolation failure: childA has ExecutionID = %q, want 'exec-task-A'", gotA)
	}
	if gotB != "exec-task-B" {
		t.Fatalf("scope isolation failure: childB has ExecutionID = %q, want 'exec-task-B'", gotB)
	}
}

func TestWithExecutionID_DataRace(t *testing.T) {
	t.Parallel()

	parentCtx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := range workers {
		go func(id int) {
			defer wg.Done()
			subCtx := xctx.WithExecutionID(parentCtx, fmt.Sprintf("parallel-exec-%d", id))
			_ = xctx.ExecutionIDFrom(subCtx)
			_ = xctx.RootExecutionIDFrom(subCtx)
		}(i)
	}

	wg.Wait()
}
