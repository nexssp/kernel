package action

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/xerr"
)

type flightDoneSignalContext struct {
	context.Context
	once  sync.Once
	ready chan struct{}
}

func (c *flightDoneSignalContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.ready) })
	return c.Context.Done()
}

func TestIdempotencyFlightLeaderCancellationIsShared(t *testing.T) {
	group := newIdempotencyFlightGroup(4)
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	started := make(chan struct{})
	leaderResult := make(chan error, 1)
	go func() {
		_, err := group.Do(leaderCtx, "same-key", func(workCtx context.Context) (any, error) {
			close(started)
			<-workCtx.Done()
			return nil, workCtx.Err()
		})
		leaderResult <- err
	}()
	<-started

	followerBase, cancelFollower := context.WithCancel(context.Background())
	defer cancelFollower()
	followerCtx := &flightDoneSignalContext{Context: followerBase, ready: make(chan struct{})}
	followerResult := make(chan error, 1)
	go func() {
		_, err := group.Do(followerCtx, "same-key", func(context.Context) (any, error) {
			return nil, errors.New("follower unexpectedly started a second execution")
		})
		followerResult <- err
	}()
	<-followerCtx.ready // The follower is waiting before the leader is canceled.

	cancelLeader()
	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	if err := <-followerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("follower error = %v, want the leader's context.Canceled result", err)
	}
	waitForFlightCount(t, group, 0)
}

func TestIdempotencyFlightFollowerCancellationDoesNotCancelLeader(t *testing.T) {
	group := newIdempotencyFlightGroup(4)
	started := make(chan struct{})
	release := make(chan struct{})
	leaderResult := make(chan error, 1)
	go func() {
		value, err := group.Do(context.Background(), "same-key", func(workCtx context.Context) (any, error) {
			close(started)
			select {
			case <-workCtx.Done():
				return nil, workCtx.Err()
			case <-release:
				return "shared-result", nil
			}
		})
		if err == nil && value != "shared-result" {
			err = errors.New("leader received an unexpected result")
		}
		leaderResult <- err
	}()
	<-started

	followerBase, cancelFollower := context.WithCancel(context.Background())
	followerCtx := &flightDoneSignalContext{Context: followerBase, ready: make(chan struct{})}
	followerResult := make(chan error, 1)
	go func() {
		_, err := group.Do(followerCtx, "same-key", func(context.Context) (any, error) {
			return nil, errors.New("follower unexpectedly started a second execution")
		})
		followerResult <- err
	}()
	<-followerCtx.ready
	cancelFollower()
	if err := <-followerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled follower error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-leaderResult; err != nil {
		t.Fatalf("leader was affected by follower cancellation: %v", err)
	}
}

func TestIdempotencyFlightCapacityBoundsUniqueWork(t *testing.T) {
	group := newIdempotencyFlightGroup(1)
	started := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		_, err := group.Do(context.Background(), "first", func(context.Context) (any, error) {
			close(started)
			<-release
			return "done", nil
		})
		firstResult <- err
	}()
	<-started

	var secondExecutions atomic.Int32
	_, err := group.Do(context.Background(), "second", func(context.Context) (any, error) {
		secondExecutions.Add(1)
		return nil, nil
	})
	if err == nil || xerr.KindFrom(err) != xerr.KindUnavailable {
		t.Fatalf("capacity error = %v, want Unavailable", err)
	}
	if secondExecutions.Load() != 0 {
		t.Fatal("handler ran despite the active-flight capacity limit")
	}

	close(release)
	if err := <-firstResult; err != nil {
		t.Fatalf("first flight error = %v", err)
	}
}

func TestIdempotencyFlightGoexitDoesNotWedgeKey(t *testing.T) {
	group := newIdempotencyFlightGroup(1)
	started := make(chan struct{})
	leaderExited := make(chan struct{})
	go func() {
		defer close(leaderExited)
		_, _ = group.Do(context.Background(), "goexit", func(context.Context) (any, error) {
			close(started)
			runtime.Goexit()
			return nil, nil
		})
	}()
	<-started
	<-leaderExited
	waitForFlightCount(t, group, 0)

	value, err := group.Do(context.Background(), "goexit", func(context.Context) (any, error) {
		return "recovered", nil
	})
	if err != nil || value != "recovered" {
		t.Fatalf("subsequent result = (%v, %v), want (recovered, nil)", value, err)
	}
}

func TestIdempotencyFlightFollowerSharesMutableResultReference(t *testing.T) {
	type mutableResult struct {
		headers map[string]string
	}

	shared := &mutableResult{headers: map[string]string{"content-type": "text/plain"}}
	flight := &idempotencyFlight{done: make(chan struct{}), value: shared}
	close(flight.done)
	group := newIdempotencyFlightGroup(1)
	group.flights["shared-key"] = flight

	var leaderStarted bool
	value, err := group.Do(context.Background(), "shared-key", func(context.Context) (any, error) {
		leaderStarted = true
		return nil, nil
	})
	if err != nil {
		t.Fatalf("follower error = %v", err)
	}
	if leaderStarted {
		t.Fatal("follower started another execution")
	}
	got, ok := value.(*mutableResult)
	if !ok || got != shared {
		t.Fatalf("follower result = %T (%p), want shared reference %p", value, got, shared)
	}
	got.headers["x-request-id"] = "follower"
	if shared.headers["x-request-id"] != "follower" {
		t.Fatal("follower result did not share the leader's mutable response data")
	}
}

func FuzzIdempotencyFlightGroupSharesOneExecution(f *testing.F) {
	f.Add("same-key", uint8(0))
	f.Add("same\x00key", uint8(4))
	f.Add("", uint8(7))

	f.Fuzz(func(t *testing.T, key string, callerSeed uint8) {
		if len(key) > 128 {
			key = key[:128]
		}
		callers := int(callerSeed%8) + 1
		group := newIdempotencyFlightGroup(1)
		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()

		var executions atomic.Int32
		var startedOnce sync.Once
		type result struct {
			value any
			err   error
		}
		results := make(chan result, callers)
		run := func(ctx context.Context) {
			value, err := group.Do(ctx, key, func(context.Context) (any, error) {
				executions.Add(1)
				startedOnce.Do(func() { close(started) })
				<-release
				return key, nil
			})
			results <- result{value: value, err: err}
		}

		go run(context.Background())
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("flight leader did not start")
		}

		for range callers - 1 {
			followerCtx := &flightDoneSignalContext{Context: context.Background(), ready: make(chan struct{})}
			go run(followerCtx)
			select {
			case <-followerCtx.ready:
			case <-time.After(5 * time.Second):
				t.Fatal("follower did not join the in-flight call")
			}
		}

		unblock()
		for range callers {
			select {
			case got := <-results:
				if got.err != nil || got.value != key {
					t.Fatalf("flight result = (%v, %v), want (%q, nil)", got.value, got.err, key)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("caller did not receive the shared result")
			}
		}
		if got := executions.Load(); got != 1 {
			t.Fatalf("shared execution count = %d, want 1", got)
		}
		waitForFlightCount(t, group, 0)
	})
}

func waitForFlightCount(t *testing.T, group *idempotencyFlightGroup, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		group.mu.Lock()
		got := len(group.flights)
		group.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("active flight count = %d, want %d", got, want)
		}
		runtime.Gosched()
	}
}
