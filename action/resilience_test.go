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

func TestRetryMiddleware(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	act := action.New("test.retry", func(ctx context.Context, req struct{}) (string, error) {
		if attempts.Add(1) < 3 {
			// Transient error triggers retry
			return "", xerr.Unavailable("temporary glitch")
		}
		return "success", nil
	}).Retry(3, action.ConstantBackoff(1*time.Millisecond)).Build()

	res, err := act.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "success" {
		t.Fatalf("expected 'success', got %v", res)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts.Load())
	}
}

func TestConcurrencyLimit(t *testing.T) {
	t.Parallel()

	inProgress := make(chan struct{})
	act := action.New("test.limit", func(ctx context.Context, req struct{}) (string, error) {
		<-inProgress // block
		return "ok", nil
	}).ConcurrencyLimit(1).Build()

	// Fill the single concurrency slot
	done := make(chan error, 1)
	go func() {
		_, err := act.Do(context.Background(), struct{}{})
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)

	// Second call should fail immediately
	_, err := act.Do(context.Background(), struct{}{})
	if !errors.Is(err, action.ErrConcurrencyLimit) {
		t.Fatalf("expected ErrConcurrencyLimit, got %v", err)
	}

	close(inProgress) // release
	if err := <-done; err != nil {
		t.Fatalf("unexpected error from first call: %v", err)
	}
}

func TestRateLimit(t *testing.T) {
	t.Parallel()

	act := action.New("test.rate", func(ctx context.Context, req struct{}) (string, error) {
		return "ok", nil
	}).RateLimit(10, 2).Build() // 10 rps, burst of 2

	if _, err := act.Do(context.Background(), struct{}{}); err != nil {
		t.Fatalf("first call should succeed, got %v", err)
	}
	if _, err := act.Do(context.Background(), struct{}{}); err != nil {
		t.Fatalf("second call should succeed, got %v", err)
	}

	// 3rd rapid call should fail
	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected rate limit error, got nil")
	}
	var appErr *xerr.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected rate limit error, got %v", err)
	}
}

func TestBuilder_Resilient_Bundle(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32

	act := action.New("stripe.charge", func(ctx context.Context, req struct{}) (string, error) {
		attempts.Add(1)
		return "", xerr.Unavailable("stripe gateway timeout")
	}).Resilient(action.ResilienceConfig{
		MaxRetries:    2,
		Timeout:       1 * time.Second,
		MaxConcurrent: 50,
	}).Build()

	if _, err := act.Do(context.Background(), struct{}{}); err == nil {
		t.Fatal("expected error after retries exhausted, got nil")
	}

	// Original attempt + 2 retries = 3 total invocations
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts via Resilient bundle, got %d", attempts.Load())
	}
}
