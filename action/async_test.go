package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestAsync(t *testing.T) {
	t.Parallel()

	act := action.New("async.test", func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	}).Build()

	ch := action.Async(context.Background(), act, 21)
	res := <-ch

	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != 42 {
		t.Fatalf("expected 42, got %d", res.Value)
	}
}

func TestFanOut(t *testing.T) {
	t.Parallel()

	act := action.New("fanout.test", func(ctx context.Context, req int) (int, error) {
		return req * 10, nil
	}).Build()

	reqs := []int{1, 2, 3, 4, 5}
	results := action.FanOut(context.Background(), act, reqs, 2)

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("unexpected error at index %d: %v", i, r.Err)
		}
		if r.Value != reqs[i]*10 {
			t.Fatalf("expected %d, got %d", reqs[i]*10, r.Value)
		}
	}
}

func TestRace(t *testing.T) {
	t.Parallel()

	act := action.New("race.test", func(ctx context.Context, req int) (int, error) {
		if req == 1 {
			time.Sleep(50 * time.Millisecond) // slow
			return 1, nil
		}
		if req == 2 {
			return 0, errors.New("fast failure") // fails fast
		}
		return req, nil // fast success
	}).Build()

	res, err := action.Race(context.Background(), act, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 3 {
		t.Fatalf("expected 3 (fastest success), got %d", res)
	}
}

func TestFanOut_ContextCancel(t *testing.T) {
	t.Parallel()

	// Use a channel to block the handler until we cancel.
	block := make(chan struct{})

	act := action.New("fanout.cancel", func(ctx context.Context, req int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-block:
			return req, nil
		}
	}).Build()

	ctx, cancel := context.WithCancel(context.Background())

	// Run FanOut in a goroutine and use a channel to pass back results to avoid data races.
	done := make(chan []action.AsyncResult[int])
	go func() {
		done <- action.FanOut(ctx, act, []int{1, 2, 3, 4, 5, 6}, 2)
	}()

	// Give time for at most the first 2 goroutines to start.
	time.Sleep(10 * time.Millisecond)

	cancel() // Cancel while some goroutines are still waiting to start.

	// Release all blocked goroutines to let everything unwind.
	close(block)

	// Wait for FanOut to finish completely and safely read results
	results := <-done

	foundCanceled := false
	for _, r := range results {
		if errors.Is(r.Err, context.Canceled) {
			foundCanceled = true
			break
		}
	}
	if !foundCanceled {
		t.Error("expected at least one result with context.Canceled, but none found")
	}
}
