package action_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

type sharedFlightContext struct {
	context.Context
	done       chan struct{}
	doneCalled chan struct{}
}

func newSharedFlightContext() *sharedFlightContext {
	return &sharedFlightContext{
		Context:    context.Background(),
		done:       make(chan struct{}),
		doneCalled: make(chan struct{}, 1),
	}
}

func (ctx *sharedFlightContext) Done() <-chan struct{} {
	select {
	case ctx.doneCalled <- struct{}{}:
	default:
	}
	return ctx.done
}

func (ctx *sharedFlightContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (ctx *sharedFlightContext) cancel() {
	close(ctx.done)
}

func newSharedFlightAction(
	kind string,
	options action.SharedFlightOptions,
	handler func(context.Context, string) (string, error),
) *action.BuiltAction[string, string] {
	builder := action.New("shared-flight."+kind, handler)
	keyFn := func(request string) string { return request }
	switch kind {
	case "coalesce":
		return builder.CoalesceWithOptions(action.NewCoalescer(), keyFn, options).Build()
	case "deduplicate":
		return builder.DedupWithOptions(keyFn, options).Build()
	default:
		panic("unknown shared-flight test kind")
	}
}

func TestSharedFlightWorkIgnoresCallerDeadline(t *testing.T) {
	for _, kind := range []string{"coalesce", "deduplicate"} {
		t.Run(kind, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			handlerContexts := make(chan context.Context, 1)
			act := newSharedFlightAction(kind, action.SharedFlightOptions{}, func(ctx context.Context, _ string) (string, error) {
				handlerContexts <- ctx
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-release:
					return "released", nil
				}
			})

			callerCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			callerDeadline, _ := callerCtx.Deadline()
			result := make(chan error, 1)
			go func() {
				_, err := act.Do(callerCtx, "same-key")
				result <- err
			}()

			handlerCtx := <-handlerContexts
			gotDeadline, ok := handlerCtx.Deadline()
			if !ok || time.Until(gotDeadline) <= time.Minute || gotDeadline.Equal(callerDeadline) {
				t.Fatalf("shared work deadline = %v, present=%t; want its independent timeout, not caller deadline %v", gotDeadline, ok, callerDeadline)
			}
			<-callerCtx.Done()
			if err := handlerCtx.Err(); err != nil {
				t.Fatalf("caller deadline canceled shared work: %v", err)
			}
			unblock()
			if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("leader error = %v, want context deadline exceeded", err)
			}
		})
	}
}

func TestSharedFlightConfiguredTimeoutReachesLeader(t *testing.T) {
	for _, kind := range []string{"coalesce", "deduplicate"} {
		t.Run(kind, func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			handlerContexts := make(chan context.Context, 1)
			act := newSharedFlightAction(kind, action.SharedFlightOptions{Timeout: 100 * time.Millisecond}, func(ctx context.Context, _ string) (string, error) {
				handlerContexts <- ctx
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-release:
					return "released", nil
				}
			})

			result := make(chan error, 1)
			go func() {
				_, err := act.Do(context.Background(), "same-key")
				result <- err
			}()
			handlerCtx := <-handlerContexts
			deadline, ok := handlerCtx.Deadline()
			if !ok || time.Until(deadline) > time.Second {
				t.Fatalf("configured leader deadline = %v, present=%t; want a near deadline", deadline, ok)
			}
			if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("leader error = %v, want configured timeout", err)
			}
		})
	}
}

func TestSharedFlightCanceledWaiterDoesNotCancelLeader(t *testing.T) {
	for _, kind := range []string{"coalesce", "deduplicate"} {
		t.Run(kind, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			handlerContexts := make(chan context.Context, 1)
			act := newSharedFlightAction(kind, action.SharedFlightOptions{Timeout: time.Minute}, func(ctx context.Context, _ string) (string, error) {
				handlerContexts <- ctx
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-release:
					return "leader-result", nil
				}
			})

			type callResult struct {
				value string
				err   error
			}
			leaderResult := make(chan callResult, 1)
			go func() {
				value, err := act.Do(context.Background(), "same-key")
				leaderResult <- callResult{value: value, err: err}
			}()
			leaderCtx := <-handlerContexts

			waiterCtx := newSharedFlightContext()
			waiterResult := make(chan error, 1)
			go func() {
				_, err := act.Do(waiterCtx, "same-key")
				waiterResult <- err
			}()
			<-waiterCtx.doneCalled
			waiterCtx.cancel()
			if err := <-waiterResult; !errors.Is(err, context.Canceled) {
				t.Fatalf("waiter error = %v, want context canceled", err)
			}
			if err := leaderCtx.Err(); err != nil {
				t.Fatalf("waiter cancellation canceled leader context: %v", err)
			}

			unblock()
			got := <-leaderResult
			if got.err != nil || got.value != "leader-result" {
				t.Fatalf("leader result = (%q, %v), want (%q, nil)", got.value, got.err, "leader-result")
			}
		})
	}
}
