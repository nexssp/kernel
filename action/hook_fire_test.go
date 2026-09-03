package action_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestOnRetry_HookFires(t *testing.T) {
	t.Parallel()
	var retryCalled atomic.Bool
	var attempts atomic.Int32

	act := action.New("hook.retry", func(ctx context.Context, req string) (string, error) {
		a := attempts.Add(1)
		if a == 1 {
			return "", &transientError{}
		}
		return "ok", nil
	}).
		Retry(1, action.ConstantBackoff(0)).
		HookRetry(func(ctx context.Context, req string, attempt int, err error, meta *action.Meta) {
			retryCalled.Store(true)
			if attempt != 1 {
				t.Errorf("expected attempt 1, got %d", attempt)
			}
		}).
		Build()

	_, err := act.Do(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !retryCalled.Load() {
		t.Fatal("OnRetry hook was not called")
	}
}

// transientError implements IsTransient; xerr.IsTransient relies on this dynamic interface.
type transientError struct{}

func (t *transientError) Error() string     { return "transient" }
func (t *transientError) IsTransient() bool { return true }

func TestOnDeduplicated_HookFires(t *testing.T) {
	t.Parallel()
	var dedupCalled sync.WaitGroup
	dedupCalled.Add(1) // should be called exactly once (by second caller)

	handlerStart := make(chan struct{})

	var handlerCalls atomic.Int32
	act := action.New("hook.dedup", func(ctx context.Context, req string) (string, error) {
		handlerCalls.Add(1)
		<-handlerStart // block to ensure overlap
		return "shared", nil
	}).
		Dedup(func(r string) string { return r }).
		Hook(action.Hook[string, string]{
			OnDeduplicated: func(ctx context.Context, req string, meta *action.Meta) {
				dedupCalled.Done()
			},
		}).
		Build()

	// Launch two concurrent calls
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			if _, err := act.Do(context.Background(), "key"); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	// Ensure both goroutines hit the middleware and queue up
	time.Sleep(50 * time.Millisecond)
	close(handlerStart) // Release the handler

	wg.Wait()
	dedupCalled.Wait() // wait for hook to have been called
	if calls := handlerCalls.Load(); calls != 1 {
		t.Errorf("expected 1 handler call, got %d", calls)
	}
}

func TestOnCoalesced_HookFires(t *testing.T) {
	t.Parallel()
	c := action.NewCoalescer()
	var coalescedHookCalled atomic.Bool

	// Gate: release handler only after both goroutines have entered the middleware.
	handlerStart := make(chan struct{})

	act := action.New("hook.coal", func(ctx context.Context, req string) (string, error) {
		<-handlerStart // block until released
		return "coalesced", nil
	}).
		Coalesce(c, func(r string) string { return r }).
		Hook(action.Hook[string, string]{
			OnCoalesced: func(ctx context.Context, req string, meta *action.Meta) {
				coalescedHookCalled.Store(true)
			},
		}).
		Build()

	var wg sync.WaitGroup
	wg.Add(2)

	// Both goroutines start; the first grabs the coalescer, the second will join.
	go func() {
		defer wg.Done()
		if _, err := act.Do(context.Background(), "k"); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := act.Do(context.Background(), "k"); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	// Give both goroutines a chance to reach the handler (or the coalescer).
	time.Sleep(50 * time.Millisecond)
	close(handlerStart) // unblock the handler → both get the result
	wg.Wait()

	if !coalescedHookCalled.Load() {
		t.Fatal("OnCoalesced hook was not called")
	}
}
