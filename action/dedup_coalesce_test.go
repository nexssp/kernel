package action_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestDeduplicate(t *testing.T) {
	var callCount atomic.Int32
	var keyCalls atomic.Int32

	handlerStart := make(chan struct{})

	act := action.New("dedup.test", func(_ context.Context, _ string) (string, error) {
		callCount.Add(1)
		<-handlerStart
		return "shared", nil
	}).Dedup(func(r string) string {
		keyCalls.Add(1)
		return r
	}).Build()

	var wg sync.WaitGroup
	wg.Go(func() {
		xtest.Eventually(t, 2*time.Second, func() bool { return keyCalls.Load() == 50 })
		time.Sleep(20 * time.Millisecond)
		close(handlerStart)
	})

	ktest.Simulate(t, act, "same-key", 50, func(t testing.TB, res string, err error) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if res != "shared" {
			t.Errorf("expected 'shared', got %q", res)
		}
	})

	wg.Wait()

	if count := callCount.Load(); count != 1 {
		t.Fatalf("deduplication failed: expected exactly 1 handler call, got %d", count)
	}
}

func TestCoalesce(t *testing.T) {
	c := action.NewCoalescer()
	var callCount atomic.Int32
	var keyCalls atomic.Int32

	handlerStart := make(chan struct{})

	act := action.New("coal.test", func(_ context.Context, _ string) (string, error) {
		callCount.Add(1)
		<-handlerStart
		return "coalesced", nil
	}).Coalesce(c, func(r string) string {
		keyCalls.Add(1)
		return r
	}).Build()

	var wg sync.WaitGroup
	wg.Go(func() {
		xtest.Eventually(t, 2*time.Second, func() bool { return keyCalls.Load() == 10 })
		time.Sleep(20 * time.Millisecond)
		close(handlerStart)
	})

	ktest.Simulate(t, act, "co-key", 10, func(t testing.TB, res string, err error) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if res != "coalesced" {
			t.Errorf("expected 'coalesced', got %q", res)
		}
	})

	wg.Wait()

	if count := callCount.Load(); count != 1 {
		t.Fatalf("coalescing failed: expected exactly 1 handler call, got %d", count)
	}
}

func TestDeduplicate_ContextBleeding(t *testing.T) {
	t.Parallel()

	handlerEntered := make(chan struct{})
	handlerRelease := make(chan struct{})
	var handlerCalls atomic.Int32
	var keyCalls atomic.Int32

	act := action.New("dedup.bleeding", func(ctx context.Context, _ string) (string, error) {
		handlerCalls.Add(1)
		close(handlerEntered)
		<-handlerRelease

		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "success", nil
	}).Dedup(func(r string) string {
		keyCalls.Add(1)
		return r
	}).Build()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	var err1, err2 error
	var res1, res2 string

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		res1, err1 = act.Do(ctx1, "shared-key")
		close(done1)
	}()

	<-handlerEntered

	go func() {
		res2, err2 = act.Do(ctx2, "shared-key")
		close(done2)
	}()

	xtest.Eventually(t, 2*time.Second, func() bool { return keyCalls.Load() == 2 })
	time.Sleep(20 * time.Millisecond) // ensure caller 2 reaches c.Do

	cancel1()
	close(handlerRelease)

	<-done1
	<-done2

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
