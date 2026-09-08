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
