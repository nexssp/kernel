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
