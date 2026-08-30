package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestExecutionIDContext(t *testing.T) {
	ctx := context.Background()
	if got := action.ExecutionIDFrom(ctx); got != "" {
		t.Fatalf("unexpected ID %q", got)
	}
	ctx = action.WithExecutionID(ctx, "exec-42")
	if got := action.ExecutionIDFrom(ctx); got != "exec-42" {
		t.Fatalf("got %q", got)
	}
	if got := action.ExecutionIDFrom(action.WithExecutionID(ctx, "")); got != "exec-42" {
		t.Fatalf("empty ID overwrote existing ID: %q", got)
	}
}
