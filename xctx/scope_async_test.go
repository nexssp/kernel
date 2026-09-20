package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestCloneForAsync(t *testing.T) {
	t.Parallel()

	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, scope, release := xctx.NewScope(parentCtx)
	scope.UserID = "usr_async"
	scope.Roles = []string{"admin"}

	asyncCtx := xctx.CloneForAsync(ctx)
	release() // Release parent scope back to pool immediately

	// Cancel parent AFTER clone
	cancel()

	if ctx.Err() == nil {
		t.Fatal("expected parent context to be canceled after cancel()")
	}

	if asyncCtx.Err() != nil {
		t.Fatalf("expected async context to remain uncancelled, got %v", asyncCtx.Err())
	}

	if got := xctx.UserIDFrom(asyncCtx); got != "usr_async" {
		t.Fatalf("expected cloned scope to retain UserID, got %q", got)
	}
}

func TestCloneForAsync_AddTraceWorks(t *testing.T) {
	t.Parallel()

	ctx, parentScope, release := xctx.NewScope(context.Background())
	defer release()

	asyncCtx := xctx.CloneForAsync(ctx)
	asyncScope := xctx.ScopeFrom(asyncCtx)
	if asyncScope == nil {
		t.Fatal("CloneForAsync did not install a scope")
	}

	xctx.AddTrace(asyncCtx, "bg-1")
	xctx.AddTrace(asyncCtx, "bg-2")

	if got := len(asyncScope.TraceEvents); got != 2 {
		t.Fatalf("AddTrace on clone dropped events: got %d, want 2", got)
	}
	if asyncScope.TraceEvents[0] != "bg-1" || asyncScope.TraceEvents[1] != "bg-2" {
		t.Fatalf("trace order/content wrong: %v", asyncScope.TraceEvents)
	}

	// Writes must not leak back into the parent scope.
	if got := len(parentScope.TraceEvents); got != 0 {
		t.Fatalf("AddTrace on clone leaked into parent: got %d", got)
	}
}
