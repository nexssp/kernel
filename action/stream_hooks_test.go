package action_test

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestStreamHookLifecycleAndItems(t *testing.T) {
	var started, items, success, after atomic.Int64
	stream := action.NewStream("hooked", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			yield(2, nil)
		}, nil
	}).UseStream(action.StreamHook[struct{}, int]{
		OnStart:   func(context.Context, struct{}, *action.Meta) { started.Add(1) },
		OnItem:    func(context.Context, struct{}, int, error, *action.Meta) { items.Add(1) },
		OnSuccess: func(context.Context, struct{}, *action.Meta) { success.Add(1) },
		After:     func(context.Context, struct{}, error, *action.Meta) { after.Add(1) },
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	seq(func(int, error) bool { return true })
	if started.Load() != 1 || items.Load() != 2 || success.Load() != 1 || after.Load() != 1 {
		t.Fatalf("started=%d items=%d success=%d after=%d", started.Load(), items.Load(), success.Load(), after.Load())
	}
}

func TestStreamHookTimeoutAndCancel(t *testing.T) {
	for name, makeCtx := range map[string]func() (context.Context, context.CancelFunc){
		"timeout": func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			<-ctx.Done()
			return ctx, cancel
		},
		"cancel": func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := makeCtx()
			defer cancel()
			if name == "cancel" {
				cancel()
			}
			var errorsSeen, timeoutSeen, cancelSeen atomic.Int64
			stream := action.NewStream("cancelable", func(ctx context.Context, _ struct{}) (iter.Seq2[int, error], error) {
				return func(yield func(int, error) bool) {
					if err := ctx.Err(); err != nil {
						yield(0, err)
					}
				}, nil
			}).UseStream(action.StreamHook[struct{}, int]{
				OnError:   func(context.Context, struct{}, error, *action.Meta) { errorsSeen.Add(1) },
				OnTimeout: func(context.Context, struct{}, *action.Meta) { timeoutSeen.Add(1) },
				OnCancel:  func(context.Context, struct{}, *action.Meta) { cancelSeen.Add(1) },
			})
			seq, err := stream.Do(ctx, struct{}{})
			if err != nil {
				t.Fatal(err)
			}
			for _, itemErr := range seq {
				if itemErr != nil && !errors.Is(itemErr, ctx.Err()) {
					t.Fatal(itemErr)
				}
			}
			if errorsSeen.Load() != 1 {
				t.Fatalf("OnError=%d, want 1", errorsSeen.Load())
			}
			if name == "timeout" && timeoutSeen.Load() != 1 {
				t.Fatalf("timeout=%d", timeoutSeen.Load())
			}
			if name == "cancel" && cancelSeen.Load() != 1 {
				t.Fatalf("cancel=%d", cancelSeen.Load())
			}
		})
	}
}

func TestStreamHookTimeoutOrderingAndError(t *testing.T) {
	var order []string
	stream := action.NewStream("timeout.order", func(ctx context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(0, ctx.Err())
		}, nil
	}).UseStream(action.StreamHook[struct{}, int]{
		OnTimeout: func(context.Context, struct{}, *action.Meta) { order = append(order, "timeout") },
		OnError:   func(context.Context, struct{}, error, *action.Meta) { order = append(order, "error") },
		After:     func(context.Context, struct{}, error, *action.Meta) { order = append(order, "after") },
	})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	seq, err := stream.Do(ctx, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	seq(func(int, error) bool { return true })
	if got, want := len(order), 3; got != want || order[0] != "timeout" || order[1] != "error" || order[2] != "after" {
		t.Fatalf("order=%v, want [timeout error after]", order)
	}
}

func TestStreamHookEarlyStopDoesNotReportUndeliveredItem(t *testing.T) {
	var items, success, after atomic.Int64
	stream := action.NewStream("early.stop", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			for i := range 3 {
				if !yield(i, nil) {
					return
				}
			}
		}, nil
	}).UseStream(action.StreamHook[struct{}, int]{
		OnItem:    func(context.Context, struct{}, int, error, *action.Meta) { items.Add(1) },
		OnSuccess: func(context.Context, struct{}, *action.Meta) { success.Add(1) },
		After:     func(context.Context, struct{}, error, *action.Meta) { after.Add(1) },
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	seq(func(int, error) bool { return false })
	if items.Load() != 0 || success.Load() != 1 || after.Load() != 1 {
		t.Fatalf("items=%d success=%d after=%d", items.Load(), success.Load(), after.Load())
	}
}
