package action_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestSaga_PreCanceledContextDoesNotStartForwardStep(t *testing.T) {
	tests := []struct {
		name     string
		optional bool
	}{
		{name: "mandatory"},
		{name: "optional", optional: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			forwardCalls := 0
			undoCalls := 0
			do := func(context.Context, string) (string, error) {
				forwardCalls++
				return "unexpected", nil
			}
			undo := func(context.Context, string) error {
				undoCalls++
				return nil
			}
			builder := action.NewSaga[string, string](t.Name())
			if test.optional {
				builder.AddOptionalStep("first", do, undo)
			} else {
				builder.AddStep("first", do, undo)
			}

			result, err := builder.Build().Do(ctx, "request")
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Saga error = %v, want context.Canceled", err)
			}
			if result.Success {
				t.Fatal("Saga succeeded with an already canceled context")
			}
			if forwardCalls != 0 || undoCalls != 0 {
				t.Fatalf("handler calls: forward=%d undo=%d, want 0 and 0", forwardCalls, undoCalls)
			}
			if result.RolledBack || result.StepsCount != 1 {
				t.Fatalf("Saga result = %+v, want failed first step without rollback", result)
			}
			step := result.Step(0)
			if step.Step != "first" || step.Success || step.Skipped || step.Error != context.Canceled.Error() {
				t.Fatalf("first step result = %+v, want cancellation failure, not optional skip", step)
			}
		})
	}
}
