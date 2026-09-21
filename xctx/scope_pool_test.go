package xctx_test

import (
	"context"
	"sync"
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

	_, scope, freshCleanup := xctx.NewScope(context.Background())
	defer freshCleanup()

	xctx.AddTrace(ctx, "stale-event")
	if len(scope.TraceEvents) != 0 {
		t.Fatalf("stale AddTrace leaked into pooled scope: %v", scope.TraceEvents)
	}
}

// TestWithScope_ConcurrentOnFreshScope exercises the CAS-protected
// generation assignment. Pre-PR-4 this was a data race under -race and
// the loser's context ended up with a stale generation, so its AddTrace
// calls silently vanished. Post-fix, every goroutine observes the same
// generation and every AddTrace lands.
func TestWithScope_ConcurrentOnFreshScope(t *testing.T) {
	t.Parallel()

	const workers = 32

	// A scope that has never been bound. Its generation is 0.
	fresh := &xctx.RequestScope{}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			ctx := xctx.WithScope(context.Background(), fresh)
			xctx.AddTrace(ctx, "event")
		})
	}
	close(start)
	wg.Wait()

	if got := len(fresh.TraceEvents); got != workers {
		t.Fatalf("TraceEvents len = %d, want %d "+
			"(generation race or CAS failure)", got, workers)
	}
}
