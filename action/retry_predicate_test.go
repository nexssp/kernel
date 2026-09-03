package action_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type customSDKError struct {
	Code int
	Msg  string
}

func (e *customSDKError) Error() string {
	return e.Msg
}

func TestRetryMiddleware_BackwardCompatible(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	mw := action.RetryMiddleware[string, string](
		2,
		action.ConstantBackoff(time.Millisecond),
	)

	next := func(ctx context.Context, req string) (string, error) {
		if calls.Add(1) < 2 {
			return "", xerr.Unavailable("transient")
		}
		return "ok", nil
	}

	wrapped := mw(next, nil)

	res, err := wrapped(context.Background(), "test")
	if err != nil || res != "ok" {
		t.Fatalf("res=%q err=%v", res, err)
	}

	if calls.Load() != 2 {
		t.Fatalf("expected 2 calls, got %d", calls.Load())
	}
}

func TestRetry_DefensiveSafeguards(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	// Negative retry count clamped to 0, nil backoff defaults to ConstantBackoff(0)
	mw := action.RetryWithPredicateMiddleware[string, string](
		-5,
		nil,
		action.AlwaysRetryPredicate,
	)

	next := func(ctx context.Context, req string) (string, error) {
		attempts.Add(1)
		return "", errors.New("err")
	}

	wrapped := mw(next, nil)

	_, err := wrapped(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Clamped maxRetry=0 should result in exactly 1 execution attempt
	if attempts.Load() != 1 {
		t.Fatalf("expected 1 attempt with clamped maxRetry, got %d", attempts.Load())
	}
}

func TestRetryIf_CustomCondition(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	act := action.New("custom.sdk.retry", func(ctx context.Context, req string) (string, error) {
		a := attempts.Add(1)
		if a < 3 {
			// Plain 3rd-party error struct
			return "", &customSDKError{Code: 503, Msg: "gateway down"}
		}
		return "recovered", nil
	}).
		RetryIf(3, action.ConstantBackoff(1*time.Millisecond), func(err error) bool {
			var sdkErr *customSDKError
			return errors.As(err, &sdkErr) && sdkErr.Code >= 500
		}).
		Build()

	res, err := act.Do(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "recovered" {
		t.Fatalf("expected 'recovered', got %q", res)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestRetryIf_SkipsOnPredicateFalse(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	act := action.New("custom.sdk.noretry", func(ctx context.Context, req string) (string, error) {
		attempts.Add(1)
		return "", &customSDKError{Code: 400, Msg: "invalid client request"}
	}).
		RetryIf(3, action.ConstantBackoff(1*time.Millisecond), func(err error) bool {
			var sdkErr *customSDKError
			return errors.As(err, &sdkErr) && sdkErr.Code >= 500
		}).
		Build()

	_, err := act.Do(context.Background(), "test")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var sdkErr *customSDKError
	if !errors.As(err, &sdkErr) || sdkErr.Code != 400 {
		t.Fatalf("expected customSDKError with code 400, got %v", err)
	}

	if attempts.Load() != 1 {
		t.Fatalf("expected exactly 1 attempt (no retry for 400), got %d", attempts.Load())
	}
}

func TestRetryAll_RetriesNonTransientErrors(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	act := action.New("script.retryall", func(ctx context.Context, req string) (string, error) {
		a := attempts.Add(1)
		if a < 3 {
			return "", errors.New("raw unclassified error from os.Exec")
		}
		return "script_done", nil
	}).
		RetryAll(3, action.ConstantBackoff(1*time.Millisecond)).
		Build()

	res, err := act.Do(context.Background(), "input")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "script_done" {
		t.Fatalf("expected 'script_done', got %q", res)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts with RetryAll, got %d", attempts.Load())
	}
}

func TestRetry_DefaultSafetyInvariantPreserved(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	act := action.New("safety.default", func(ctx context.Context, req string) (string, error) {
		attempts.Add(1)
		return "", errors.New("unclassified plain error")
	}).
		Retry(3, action.ConstantBackoff(1*time.Millisecond)).
		Build()

	_, err := act.Do(context.Background(), "input")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected default Retry() to exit immediately on permanent error, got %d attempts", attempts.Load())
	}
}

func TestResilienceConfig_WithCustomPredicate(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	act := action.New("resilience.bundle.predicate", func(ctx context.Context, req string) (string, error) {
		a := attempts.Add(1)
		if a < 2 {
			return "", errors.New("custom retryable error")
		}
		return "ok", nil
	}).
		Resilient(action.ResilienceConfig{
			MaxRetries: 2,
			Backoff:    action.ConstantBackoff(1 * time.Millisecond),
			Predicate: func(err error) bool {
				return err != nil && err.Error() == "custom retryable error"
			},
		}).
		Build()

	res, err := act.Do(context.Background(), "input")
	if err != nil || res != "ok" {
		t.Fatalf("expected success on 2nd attempt, res=%q err=%v", res, err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
}
