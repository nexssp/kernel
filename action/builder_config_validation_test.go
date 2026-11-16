package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestBuilderValidateConfig(t *testing.T) {
	t.Run("request validation remains deferred", func(t *testing.T) {
		var calls int
		builder := action.New("builder.request-validation", func(context.Context, int) (int, error) {
			return 1, nil
		}).Validate(func(context.Context, int) error {
			calls++
			return nil
		})

		if err := builder.ValidateConfig(); err != nil {
			t.Fatalf("valid builder configuration error = %v", err)
		}
		if calls != 0 {
			t.Fatalf("request validator ran %d times during config validation", calls)
		}
		if _, err := builder.Build().Do(context.Background(), 1); err != nil {
			t.Fatalf("Do returned error: %v", err)
		}
		if calls != 1 {
			t.Fatalf("request validator ran %d times during Do, want 1", calls)
		}
	})

	t.Run("deferred resilience error", func(t *testing.T) {
		builder := action.New("builder.resilience", func(context.Context, int) (int, error) {
			return 1, nil
		}).Resilient(action.ResilienceConfig{MaxRetries: -1})

		assertBuilderConfigErrorMatchesDo(t, builder, 1)
	})

	t.Run("deferred history error", func(t *testing.T) {
		builder, history := action.New("builder.history", func(context.Context, oversizedHistoryRequest) (int, error) {
			return 1, nil
		}).WithHistory(oversizedHistoryCapacity())
		if history != nil {
			t.Fatal("over-budget history allocation unexpectedly returned a history")
		}

		assertBuilderConfigErrorMatchesDo(t, builder, oversizedHistoryRequest{})
	})
}

func assertBuilderConfigErrorMatchesDo[Req, Res any](t *testing.T, builder *action.Builder[Req, Res], req Req) {
	t.Helper()

	configErr := builder.ValidateConfig()
	if configErr == nil {
		t.Fatal("ValidateConfig returned nil for a deferred configuration error")
	}
	if _, doErr := builder.Build().Do(context.Background(), req); doErr == nil {
		t.Fatal("Build().Do returned nil for a deferred configuration error")
	} else if doErr.Error() != configErr.Error() {
		t.Fatalf("Build().Do error = %q, ValidateConfig error = %q", doErr, configErr)
	}
}
