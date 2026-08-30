package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestTraceContext(t *testing.T) {
	ctx := action.WithTraceContext(context.Background(), "trace-1", "span-2")
	if action.TraceIDFrom(ctx) != "trace-1" || action.SpanIDFrom(ctx) != "span-2" {
		t.Fatalf("trace=%q span=%q", action.TraceIDFrom(ctx), action.SpanIDFrom(ctx))
	}
	if action.TraceIDFrom(action.WithTraceContext(ctx, "", "")) != "trace-1" {
		t.Fatal("empty trace context overwrote existing context")
	}
}
