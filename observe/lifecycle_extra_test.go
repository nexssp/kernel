package observe_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/observe"
)

func TestHook_AllActionLifecycleCallbacks(t *testing.T) {
	sink := &eventCollector{}
	hook := observe.Hook(sink)
	meta := &action.Meta{Name: "orders.create"}
	ctx := action.WithExecutionID(context.Background(), "exec-1")

	hook.OnCancel(ctx, "req", meta)
	hook.OnPanic(ctx, "req", "boom", meta)
	hook.OnCoalesced(ctx, "req", meta)
	hook.OnDeduplicated(ctx, "req", meta)

	events := sink.snapshot()
	if len(events) != 4 {
		t.Fatalf("got %d events, want 4", len(events))
	}
	want := []string{observe.KindCanceled, observe.KindPanic, observe.KindCoalesced, observe.KindDeduplicated}
	for i, kind := range want {
		if events[i].Kind != kind {
			t.Errorf("event %d: got %q, want %q", i, events[i].Kind, kind)
		}
		if events[i].Action != meta.Name || events[i].ExecutionID != "exec-1" {
			t.Errorf("event %d correlation/action mismatch: %+v", i, events[i])
		}
	}
	if events[1].Recovered != "boom" {
		t.Fatalf("panic payload: got %#v, want %q", events[1].Recovered, "boom")
	}
}

func TestHook_CancellationDoesNotBecomeError(t *testing.T) {
	sink := &eventCollector{}
	hook := observe.Hook(sink)
	hook.OnCancel(context.Background(), "req", &action.Meta{Name: "cancel.action"})

	events := sink.snapshot()
	if len(events) != 1 || events[0].Kind != observe.KindCanceled {
		t.Fatalf("got %+v, want one canceled event", events)
	}
	if errors.Is(events[0].Error, context.Canceled) {
		t.Fatal("cancellation event should not duplicate the error field")
	}
}

func TestHook_CapturesActionDuration(t *testing.T) {
	sink := &eventCollector{}
	act := action.New("slow.action", func(context.Context, string) (string, error) {
		time.Sleep(10 * time.Millisecond) // Ensure the OS clock advances beyond tick granularity
		return "ok", nil
	}).AnyHook(observe.Hook(sink)).Build()

	if _, err := act.Do(context.Background(), "request"); err != nil {
		t.Fatal(err)
	}
	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Duration <= 0 {
		t.Fatalf("got duration %s, want positive duration", events[0].Duration)
	}
}
