package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestTimeoutZeroDisablesDeadline(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, _ struct{}) (string, error) {
		if _, ok := ctx.Deadline(); ok {
			return "", errors.New("unexpected deadline")
		}
		return "ok", nil
	}
	assertNoDeadline := func(t *testing.T, act *action.BuiltAction[struct{}, string]) {
		t.Helper()
		got, err := act.Do(context.Background(), struct{}{})
		if err != nil || got != "ok" {
			t.Fatalf("Do() = %q, %v; want ok, nil", got, err)
		}
	}

	t.Run("builder", func(t *testing.T) {
		act := action.New("timeout.zero.builder", handler).Timeout(0).Build()
		assertNoDeadline(t, act)
	})
	t.Run("middleware", func(t *testing.T) {
		act := action.New("timeout.zero.middleware", handler).
			Use(action.TimeoutMiddleware[struct{}, string](0)).Build()
		assertNoDeadline(t, act)
	})
	t.Run("resilience config", func(t *testing.T) {
		b := action.New("timeout.zero.resilience", handler)
		if err := b.ApplyResilience(action.ResilienceConfig{Timeout: 0}); err != nil {
			t.Fatalf("ApplyResilience() error = %v", err)
		}
		assertNoDeadline(t, b.Build())
	})
	t.Run("DSL", func(t *testing.T) {
		act := action.New("timeout.zero.dsl", handler).
			WithDSL(action.DSLModifiers{Timeout: 0}).Build()
		assertNoDeadline(t, act)
	})
}

func TestTimeoutZeroDoesNotRemoveEarlierTimeout(t *testing.T) {
	t.Parallel()

	deadlineSeen := false
	handler := func(ctx context.Context, _ struct{}) (string, error) {
		if _, ok := ctx.Deadline(); !ok {
			return "", errors.New("earlier timeout deadline was removed")
		}
		deadlineSeen = true
		return "ok", nil
	}
	act := action.New("timeout.zero.after.positive", handler).
		Timeout(time.Second).
		Timeout(0).
		Build()

	if got := act.GetMeta().Timeout; got != 0 {
		t.Fatalf("metadata timeout = %s, want reset to 0", got)
	}
	got, err := act.Do(context.Background(), struct{}{})
	if err != nil || got != "ok" {
		t.Fatalf("Do() = %q, %v; want ok, nil", got, err)
	}
	if !deadlineSeen {
		t.Fatal("the earlier timeout middleware did not run")
	}
}

func TestTimeoutNegativeValuesAreRejected(t *testing.T) {
	t.Parallel()

	handler := func(context.Context, struct{}) (string, error) { return "ok", nil }
	assertValidationError := func(t *testing.T, err error) {
		t.Helper()
		if kind := xerr.KindFrom(err); kind != xerr.KindValidation {
			t.Fatalf("error kind = %q, want %q (err: %v)", kind, xerr.KindValidation, err)
		}
	}

	t.Run("builder", func(t *testing.T) {
		b := action.New("timeout.negative.builder", handler).Timeout(-time.Second)
		assertValidationError(t, b.ValidateConfig())
		_, err := b.Build().Do(context.Background(), struct{}{})
		assertValidationError(t, err)
	})
	t.Run("middleware", func(t *testing.T) {
		act := action.New("timeout.negative.middleware", handler).
			Use(action.TimeoutMiddleware[struct{}, string](-time.Second)).Build()
		_, err := act.Do(context.Background(), struct{}{})
		assertValidationError(t, err)
	})
	t.Run("DSL", func(t *testing.T) {
		b := action.New("timeout.negative.dsl", handler).
			WithDSL(action.DSLModifiers{Timeout: -time.Second})
		assertValidationError(t, b.ValidateConfig())
	})
	t.Run("resilience config", func(t *testing.T) {
		b := action.New("timeout.negative.resilience", handler)
		err := b.ApplyResilience(action.ResilienceConfig{Timeout: -time.Second})
		assertValidationError(t, err)
	})
}
