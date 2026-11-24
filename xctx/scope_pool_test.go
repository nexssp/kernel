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

func TestAddTraceConcurrentWithCleanup(_ *testing.T) {
	const (
		iterations = 200
		writers    = 8
		writes     = 16
	)

	for range iterations {
		ctx, _, cleanup := xctx.NewScope(context.Background())
		for range xctx.MaxTraceEvents {
			xctx.AddTrace(ctx, "seed")
		}

		start := make(chan struct{})
		var workers sync.WaitGroup
		for range writers {
			workers.Go(func() {
				<-start
				for range writes {
					xctx.AddTrace(ctx, "concurrent")
				}
			})
		}
		workers.Go(func() {
			<-start
			cleanup()
		})
		close(start)
		workers.Wait()
		// cleanup returned the scope to sync.Pool; do not inspect its pointer.
	}
}

func TestRequestScopeResetClearsReferences(t *testing.T) {
	t.Parallel()

	traceEvents := []string{
		"previous-request-event",
		"retained-event-1",
		"retained-event-2",
		"retained-event-3",
	}
	scope := &xctx.RequestScope{
		RequestID:      "request-id",
		IdempotencyKey: "idempotency-key",
		TraceID:        "trace-id",
		SpanID:         "span-id",
		Endpoint:       "/endpoint",
		UserID:         "user-id",
		TenantID:       "tenant-id",
		Role:           "admin",
		ClientIP:       "192.0.2.1",
		Roles:          []string{"admin"},
		Permissions:    []string{"invoice:read"},
		Features:       []string{"audit-log"},
		TraceEvents:    traceEvents[:1],
	}

	scope.Reset()

	if scope.RequestID != "" || scope.IdempotencyKey != "" || scope.TraceID != "" || scope.SpanID != "" || scope.Endpoint != "" || scope.UserID != "" || scope.TenantID != "" || scope.Role != "" || scope.ClientIP != "" {
		t.Fatal("expected scalar scope fields to be reset")
	}
	if scope.Roles != nil || scope.Permissions != nil || scope.Features != nil {
		t.Fatal("expected authorization slices to be released")
	}
	if len(scope.TraceEvents) != 0 {
		t.Fatal("expected trace-event slice to be reset")
	}
	for i, event := range traceEvents {
		if event != "" {
			t.Errorf("expected trace-event backing element %d to be cleared, got %q", i, event)
		}
	}
}

func TestRequestScopeResetDiscardsLargeTraceEventBuffer(t *testing.T) {
	t.Parallel()

	scope := &xctx.RequestScope{TraceEvents: make([]string, 1, xctx.MaxTraceEvents+1)}
	scope.Reset()
	if scope.TraceEvents != nil {
		t.Fatal("expected oversized trace-event buffer to be discarded")
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
