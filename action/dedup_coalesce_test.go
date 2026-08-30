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

func TestDeduplicate(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	startGate := make(chan struct{})
	handlerHold := make(chan struct{})

	act := action.New("dedup.test", func(ctx context.Context, req string) (string, error) {
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
			res, err := act.Do(context.Background(), "same-key")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			results[idx] = res
		}(i)
	}

	close(startGate)
	time.Sleep(50 * time.Millisecond)
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

func TestCoalesce(t *testing.T) {
	t.Parallel()
	c := action.NewCoalescer()

	var callCount atomic.Int32
	handlerHold := make(chan struct{})

	act := action.New("coal.test", func(ctx context.Context, req string) (string, error) {
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
			if _, err := act.Do(context.Background(), "co-key"); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(handlerHold)
	wg.Wait()

	if count := callCount.Load(); count != 1 {
		t.Fatalf("coalescing failed: expected exactly 1 handler call, got %d", count)
	}
}

func TestDeduplicate_ContextBleeding(t *testing.T) {
	t.Parallel()

	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	var handlerCalls int32

	act := action.New("dedup.bleeding", func(ctx context.Context, req string) (string, error) {
		atomic.AddInt32(&handlerCalls, 1)
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
