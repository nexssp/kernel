package observe_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/observe"
)

type eventCollector struct {
	mu     sync.Mutex
	events []observe.Event
}

func (c *eventCollector) Emit(_ context.Context, e observe.Event) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *eventCollector) snapshot() []observe.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]observe.Event(nil), c.events...)
}

func TestHook_AllLifecycleCallbacks(t *testing.T) {
	t.Parallel()

	collector := &eventCollector{}
	h := observe.Hook(collector)
	meta := &action.Meta{Name: "orders.create"}
	ctx := action.WithTraceContext(
		action.WithExecutionID(context.Background(), "exec-1"),
		"trace-1", "span-1",
	)

	h.OnExecuted(ctx, "req", "res", nil, meta)
	h.OnError(ctx, "req", errors.New("boom"), meta)
	h.OnRetry(ctx, "req", 2, errors.New("retry"), meta)
	h.OnCacheHit(ctx, "req", "cached", meta)
	h.OnCacheMiss(ctx, "req", meta)

	events := collector.snapshot()
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}

	assertEvent := func(index int, kind string, attempt int, hasErr bool) {
		t.Helper()
		e := events[index]
		if e.Kind != kind || e.Action != "orders.create" ||
			e.ExecutionID != "exec-1" || e.TraceID != "trace-1" ||
			e.SpanID != "span-1" || e.Attempt != attempt {
			t.Errorf("event[%d] = %+v", index, e)
		}
		if hasErr != (e.Error != nil) {
			t.Errorf("event[%d] error presence = %v, want %v", index, e.Error != nil, hasErr)
		}
	}

	assertEvent(0, observe.KindExecuted, 0, false)
	assertEvent(1, observe.KindError, 0, true)
	assertEvent(2, observe.KindRetry, 2, true)
	assertEvent(3, observe.KindCacheHit, 0, false)
	assertEvent(4, observe.KindCacheMiss, 0, false)
}

func TestHook_NilSinkReturnsNoop(t *testing.T) {
	t.Parallel()

	h := observe.Hook(nil)
	if h.OnExecuted != nil || h.OnError != nil || h.OnRetry != nil ||
		h.OnCacheHit != nil || h.OnCacheMiss != nil || h.OnCancel != nil {
		t.Fatal("nil sink must produce an empty action.AnyHook")
	}
}

func TestHook_MissingMetaUsesEmptyAction(t *testing.T) {
	t.Parallel()

	collector := &eventCollector{}
	h := observe.Hook(collector)
	h.OnExecuted(context.Background(), "req", "res", nil, nil)

	events := collector.snapshot()
	if len(events) != 1 || events[0].Action != "" {
		t.Fatalf("expected one event with empty action, got %+v", events)
	}
}
