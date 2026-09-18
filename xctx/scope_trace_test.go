package xctx_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

// TestAddTrace_ConcurrentWriters hammers the trace ring from many
// goroutines sharing a single scope. Under the previous unsynchronised
// implementation this fails under -race; with the traceMu guard it must
// be clean and land exactly MaxTraceEvents valid entries.
func TestAddTrace_ConcurrentWriters(t *testing.T) {
	ctx, scope, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			for j := range 100 {
				xctx.AddTrace(ctx, fmt.Sprintf("w%d-e%d", i, j))
			}
		})
	}
	wg.Wait()

	if got := len(scope.TraceEvents); got != xctx.MaxTraceEvents {
		t.Fatalf("TraceEvents len = %d, want %d", got, xctx.MaxTraceEvents)
	}
	// Every surviving slot must be a non-empty string. A corrupted
	// slice header (the old bug) manifests as zero-value strings in
	// the middle of the ring.
	for i, e := range scope.TraceEvents {
		if e == "" {
			t.Fatalf("TraceEvents[%d] is empty; ring shift corrupted a slot", i)
		}
	}
}

// TestAddTrace_NilScopeIsNoop verifies AddTrace survives a context with
// no bound scope and a nil context without panicking.
func TestAddTrace_NilScopeIsNoop(t *testing.T) {
	t.Parallel()

	// No scope bound — must be a silent no-op.
	xctx.AddTrace(context.Background(), "orphan")

	// Nil context — same.
	//nolint:staticcheck // explicit nil-context path
	xctx.AddTrace(nil, "orphan")
}

// TestAddTrace_RingEviction verifies the ring drops the oldest event
// once it exceeds MaxTraceEvents, keeping the newest entries.
func TestAddTrace_RingEviction(t *testing.T) {
	t.Parallel()

	ctx, scope, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	total := xctx.MaxTraceEvents + 10
	for i := range total {
		xctx.AddTrace(ctx, fmt.Sprintf("e%d", i))
	}

	if got := len(scope.TraceEvents); got != xctx.MaxTraceEvents {
		t.Fatalf("TraceEvents len = %d, want %d", got, xctx.MaxTraceEvents)
	}
	// Newest must be last, oldest surviving must be the one that
	// pushed out the initial "e0".
	if got := scope.TraceEvents[len(scope.TraceEvents)-1]; got != fmt.Sprintf("e%d", total-1) {
		t.Fatalf("last event = %q, want e%d", got, total-1)
	}
	if got := scope.TraceEvents[0]; got != "e10" {
		t.Fatalf("first surviving event = %q, want e10", got)
	}
}

func TestCloneForAsync_ConcurrentWithAddTrace(t *testing.T) {
	t.Parallel()
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	start := make(chan struct{})
	done := make(chan struct{})

	go func() {
		<-start
		for range 1000 {
			xctx.AddTrace(ctx, "concurrent-event")
		}
		close(done)
	}()

	close(start)
	for range 1000 {
		asyncCtx := xctx.CloneForAsync(ctx)
		_ = xctx.ScopeFrom(asyncCtx)
	}

	<-done
}
