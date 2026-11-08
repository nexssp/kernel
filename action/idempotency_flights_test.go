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
