package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestNewScopeCleanupIsIdempotent(t *testing.T) {
	ctx, scope, cleanup := xctx.NewScope(context.Background())
	scope.RequestID = "request-1"
	cleanup()
	cleanup()

	_, next, nextCleanup := xctx.NewScope(ctx)
	defer nextCleanup()
	if next.RequestID != "" {
		t.Fatalf("pooled scope retained request ID %q", next.RequestID)
	}
}

func TestAddTrace_AfterCleanupStaysOut(t *testing.T) {
	ctx, _, cleanup := xctx.NewScope(context.Background())
	cleanup()

	fresh, scope, freshCleanup := xctx.NewScope(context.Background())
	defer freshCleanup()

	xctx.AddTrace(ctx, "stale-event")
	if len(scope.TraceEvents) != 0 {
		t.Fatalf("stale AddTrace leaked into pooled scope: %v", scope.TraceEvents)
	}
	_ = fresh
}
