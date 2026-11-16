package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestAsync(t *testing.T) {
	t.Parallel()

	act := action.New("async.test", func(_ context.Context, req int) (int, error) {
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

func TestAsyncAPIsRejectNilAction(t *testing.T) {
	t.Parallel()

	var act *action.BuiltAction[int, int]
	assertInternal := func(name string, err error) {
		t.Helper()
		if kind := xerr.KindFrom(err); kind != xerr.KindInternal {
			t.Fatalf("%s error kind = %q, want %q", name, kind, xerr.KindInternal)
		}
	}

	results := action.Async(context.Background(), act, 1)
	result, ok := <-results
	if !ok {
		t.Fatal("Async closed without a result")
	}
	assertInternal("Async", result.Err)
	if _, ok := <-results; ok {
		t.Fatal("Async returned more than one result")
	}

	requests := []int{1, 2}
	for _, test := range []struct {
		name    string
		results []action.AsyncResult[int]
	}{
		{name: "FanOut", results: action.FanOut(context.Background(), act, requests, 1)},
		{name: "FanOutWithOptions", results: action.FanOutWithOptions(context.Background(), act, requests, action.AsyncOptions{MaxConcurrency: 1})},
	} {
		if len(test.results) != len(requests) {
			t.Fatalf("%s results = %d, want %d", test.name, len(test.results), len(requests))
		}
		for index, result := range test.results {
			assertInternal(test.name, result.Err)
			if result.Value != 0 {
				t.Fatalf("%s result[%d] = %d, want zero value", test.name, index, result.Value)
			}
		}
	}

	_, err := action.Race(context.Background(), act, requests)
	assertInternal("Race", err)
	_, err = action.RaceWithOptions(context.Background(), act, requests, action.AsyncOptions{MaxConcurrency: 1})
	assertInternal("RaceWithOptions", err)
}

func TestFanOut(t *testing.T) {
	t.Parallel()

	act := action.New("fanout.test", func(_ context.Context, req int) (int, error) {
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

	act := action.New("race.test", func(_ context.Context, req int) (int, error) {
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
	for i := range results {
		r := &results[i]
		if errors.Is(r.Err, context.Canceled) {
			foundCanceled = true
			break
		}
	}
	if !foundCanceled {
		t.Error("expected at least one result with context.Canceled, but none found")
	}
}
