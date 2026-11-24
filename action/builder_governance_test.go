package action_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestImmutableWhenConcurrentDefaultReason(t *testing.T) {
	const callers = 64
	const defaultReason = "modification forbidden: resource is locked in current state"

	guardEntered := make(chan struct{}, callers)
	guardRelease := make(chan struct{})
	act := action.New("immutable.concurrent", func(context.Context, struct{}) (string, error) {
		return "unexpected", nil
	}).ImmutableWhen(func(context.Context, struct{}) (bool, error) {
		guardEntered <- struct{}{}
		<-guardRelease
		return true, nil
	}, "").Build()

	results := make(chan error, callers)
	for range callers {
		go func() {
			_, err := act.Do(context.Background(), struct{}{})
			results <- err
		}()
	}

	for range callers {
		<-guardEntered
	}
	close(guardRelease)

	for range callers {
		err := <-results
		var appErr *xerr.AppError
		if !errors.As(err, &appErr) {
			t.Fatalf("expected forbidden AppError, got %v", err)
		}
		if appErr.Kind != xerr.KindForbidden || appErr.Message != defaultReason {
			t.Fatalf("unexpected immutable error: kind=%q message=%q", appErr.Kind, appErr.Message)
		}
	}
}
