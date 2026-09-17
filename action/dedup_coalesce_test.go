package action_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

// TestDeduplicate verifies that concurrent identical requests collapse into a
// single handler invocation.
//
// NOTE: this test intentionally does NOT run with t.Parallel(). It launches a
// large number of goroutines that must all reach act.Do() before the handler
// is released; running it alongside other goroutine-heavy tests in this
// package starves the scheduler and makes a fixed time.Sleep barrier
// unreliable. Instead we wait on an atomic counter until every goroutine has
// actually entered the call, with a generous timeout as a safety net.
func TestDeduplicate(t *testing.T) {
	var callCount atomic.Int32
	var arrived atomic.Int32
	startGate := make(chan struct{})
	handlerHold := make(chan struct{})

	act := action.New("dedup.test", func(_ context.Context, _ string) (string, error) {
		callCount.Add(1)
		<-handlerHold
		return "shared", nil
	}).Dedup(func(r string) string { return r }).Build()

	n := 50
	results := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)

	for i := range n {
		go func(idx int) {
			defer wg.Done()
			<-startGate
			arrived.Add(1)
			res, err := act.Do(context.Background(), "same-key")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			results[idx] = res
		}(i)
	}

	close(startGate)

	// Wait until every goroutine has actually entered act.Do (not a guessed
	// sleep duration) before releasing the handler. This is what makes the
	// test deterministic under scheduler contention.
	waitForCount(t, &arrived, int32(n), 5*time.Second)

	close(handlerHold)
	wg.Wait()

	if count := callCount.Load(); count != 1 {
		t.Fatalf("deduplication failed: expected exactly 1 handler call, got %d", count)
	}

	for i, r := range results {
		if r != "shared" {
			t.Fatalf("expected 'shared' at index %d, got %q", i, r)
		}
	}
}

// TestCoalesce mirrors TestDeduplicate for the Coalesce middleware. Same
// rationale: no t.Parallel(), and a counter-based barrier instead of a fixed
// sleep so the test can't false-fail just because the scheduler was slow to
// run all goroutines within an arbitrary window.
func TestCoalesce(t *testing.T) {
	c := action.NewCoalescer()

	var callCount atomic.Int32
	var arrived atomic.Int32
	handlerHold := make(chan struct{})

	act := action.New("coal.test", func(_ context.Context, _ string) (string, error) {
		callCount.Add(1)
		<-handlerHold
		return "coalesced", nil
	}).Coalesce(c, func(r string) string { return r }).Build()

	n := 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			arrived.Add(1)
			if _, err := act.Do(context.Background(), "co-key"); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	waitForCount(t, &arrived, int32(n), 5*time.Second)

	close(handlerHold)
	wg.Wait()

	if count := callCount.Load(); count != 1 {
		t.Fatalf("coalescing failed: expected exactly 1 handler call, got %d", count)
	}
}

// waitForCount polls counter until it reaches at least want, or fails the
// test after timeout. Use this instead of a fixed time.Sleep whenever a test
// needs to know that N goroutines have actually reached a checkpoint before
// proceeding — it is both faster on the happy path and far less flaky under
// CPU contention than a guessed sleep duration.
func waitForCount(t *testing.T, counter *atomic.Int32, want int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for counter to reach %d, got %d", want, counter.Load())
}

func TestDeduplicate_ContextBleeding(t *testing.T) {
	t.Parallel()

	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	var handlerCalls atomic.Int32

	act := action.New("dedup.bleeding", func(ctx context.Context, _ string) (string, error) {
		handlerCalls.Add(1)
		close(handlerEntered)
		<-handlerRelease

		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "success", nil
	}).Dedup(func(r string) string { return r }).Build()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	var err1, err2 error
	var res1, res2 string
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = act.Do(ctx1, "shared-key")
	}()

	<-handlerEntered

	go func() {
		defer wg.Done()
		res2, err2 = act.Do(ctx2, "shared-key")
	}()

	time.Sleep(50 * time.Millisecond)
	cancel1()
	close(handlerRelease)

	wg.Wait()

	if err1 == nil || !errors.Is(err1, context.Canceled) {
		t.Errorf("client 1 should receive context.Canceled, got: %v", err1)
	}
	if res1 != "" {
		t.Errorf("client 1 should receive empty result, got: %q", res1)
	}

	if err2 != nil {
		t.Fatalf("client 2 should not receive error: %v", err2)
	}
	if res2 != "success" {
		t.Errorf("client 2 expected 'success', got: %q", res2)
	}
}
