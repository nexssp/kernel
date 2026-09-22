package action_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest"
)

func TestOnRetry_HookFires(t *testing.T) {
	t.Parallel()
	var retryCalled atomic.Bool
	var attempts atomic.Int32

	act := action.New("hook.retry", func(_ context.Context, _ string) (string, error) {
		a := attempts.Add(1)
		if a == 1 {
			return "", &transientError{}
		}
		return "ok", nil
	}).
		Retry(1, action.ConstantBackoff(0)).
		HookRetry(func(_ context.Context, _ string, attempt int, _ error, _ *action.Meta) {
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

type transientError struct{}

func (t *transientError) Error() string     { return "transient" }
func (t *transientError) IsTransient() bool { return true }

func TestOnDeduplicated_HookFires(t *testing.T) {
	t.Parallel()
	var dedupCalled sync.WaitGroup
	dedupCalled.Add(1) // should be called exactly once (by second caller)

	handlerStart := make(chan struct{})

	var handlerCalls atomic.Int32
	var keyCalls atomic.Int32

	act := action.New("hook.dedup", func(_ context.Context, _ string) (string, error) {
		handlerCalls.Add(1)
		<-handlerStart // block to ensure overlap
		return "shared", nil
	}).
		Dedup(func(r string) string {
			keyCalls.Add(1)
			return r
		}).
		Hook(action.Hook[string, string]{
			OnDeduplicated: func(_ context.Context, _ string, _ *action.Meta) {
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

	// Wait deterministically for both callers to reach keyFn
	xtest.Eventually(t, 2*time.Second, func() bool { return keyCalls.Load() == 2 })
	time.Sleep(20 * time.Millisecond) // ensure both reach deduplicate lock

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
	coalescedHookCalled := make(chan struct{})

	// Gate: release handler only after both goroutines have entered the middleware.
	entered := xtest.NewLatch()
	release := make(chan struct{})
	var keyCalls atomic.Int32

	act := action.New("hook.coal", func(_ context.Context, _ string) (string, error) {
		entered.Signal()
		<-release
		return "coalesced", nil
	}).Coalesce(c, func(r string) string {
		keyCalls.Add(1)
		return r
	}).
		Hook(action.Hook[string, string]{
			OnCoalesced: func(_ context.Context, _ string, _ *action.Meta) {
				close(coalescedHookCalled)
			},
		}).Build()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); act.Do(context.Background(), "k") }()
	entered.Wait(t, 2*time.Second) // caller 1 is inside the handler

	go func() { defer wg.Done(); act.Do(context.Background(), "k") }()

	// Wait deterministically for the second caller to reach keyFn
	xtest.Eventually(t, 2*time.Second, func() bool { return keyCalls.Load() == 2 })
	time.Sleep(20 * time.Millisecond) // ensure caller 2 reaches coalescer.Do

	close(release)
	wg.Wait()

	// The hook runs after the shared result is released to the waiting caller.
	xtest.WaitForSignal(t, coalescedHookCalled, 2*time.Second)
}
