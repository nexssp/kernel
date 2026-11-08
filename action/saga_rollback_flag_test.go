package action_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestSagaRolledBackRequiresCompleteCompensation(t *testing.T) {
	type priorStep struct {
		name string
		undo func(context.Context, string) error
	}

	undoFailure := errors.New("undo failed")
	successfulUndo := func(context.Context, string) error { return nil }
	failedUndo := func(context.Context, string) error { return undoFailure }

	tests := []struct {
		name                   string
		prior                  []priorStep
		wantRolledBack         bool
		wantUndoCalls          int
		wantCompensationErrors int
	}{
		{
			name: "no prior completed steps",
		},
		{
			name:  "completed step without undo",
			prior: []priorStep{{name: "completed"}},
		},
		{
			name:                   "all prior steps compensated successfully",
			prior:                  []priorStep{{name: "completed", undo: successfulUndo}},
			wantRolledBack:         true,
			wantUndoCalls:          1,
			wantCompensationErrors: 0,
		},
		{
			name:                   "undo failed",
			prior:                  []priorStep{{name: "completed", undo: failedUndo}},
			wantUndoCalls:          1,
			wantCompensationErrors: 1,
		},
		{
			name: "some prior step lacked an undo",
			prior: []priorStep{
				{name: "undoable", undo: successfulUndo},
				{name: "not_undoable"},
			},
			wantUndoCalls: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			undoCalls := 0
			builder := action.NewSaga[string, string](t.Name())
			for _, prior := range test.prior {
				stepName := prior.name
				stepUndo := prior.undo
				do := func(context.Context, string) (string, error) {
					return stepName + "-done", nil
				}
				if stepUndo == nil {
					builder.AddStep(stepName, do, nil)
					continue
				}
				builder.AddStep(stepName, do, func(ctx context.Context, req string) error {
					undoCalls++
					return stepUndo(ctx, req)
				})
			}
			builder.AddStep("failure", func(context.Context, string) (string, error) {
				return "", errors.New("forward failure")
			}, nil)

			result, err := builder.Build().Do(context.Background(), "request")
			if err == nil {
				t.Fatal("expected the forward step to fail")
			}
			if result.RolledBack != test.wantRolledBack {
				t.Fatalf("RolledBack = %t, want %t", result.RolledBack, test.wantRolledBack)
			}
			if undoCalls != test.wantUndoCalls {
				t.Fatalf("undo calls = %d, want %d", undoCalls, test.wantUndoCalls)
			}
			if len(result.CompensationErrors) != test.wantCompensationErrors {
				t.Fatalf("compensation errors = %v, want %d errors", result.CompensationErrors, test.wantCompensationErrors)
			}
		})
	}
}
