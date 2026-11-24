package action_test

import (
	"context"
	"errors"
	"iter"
	"slices"
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

func TestStreamAction_CancellationAfterPartialOutputIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	yielded := make(chan struct{})
	type observedItem struct {
		value int
		err   error
	}
	var items []observedItem
	var success, errorsSeen, cancelSeen atomic.Int64
	var typedErrors, typedCancel atomic.Int64
	var anyErrors, anyCancel atomic.Int64
	stream := action.NewStream("cancel.partial", func(ctx context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			close(yielded)
			<-ctx.Done()
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		OnError:  func(context.Context, struct{}, error, *action.Meta) { typedErrors.Add(1) },
		OnCancel: func(context.Context, struct{}, *action.Meta) { typedCancel.Add(1) },
	}).UseStream(action.StreamHook[struct{}, int]{
		OnItem: func(_ context.Context, _ struct{}, item int, itemErr error, _ *action.Meta) {
			items = append(items, observedItem{value: item, err: itemErr})
		},
		OnSuccess: func(context.Context, struct{}, *action.Meta) { success.Add(1) },
		OnError:   func(context.Context, struct{}, error, *action.Meta) { errorsSeen.Add(1) },
		OnCancel:  func(context.Context, struct{}, *action.Meta) { cancelSeen.Add(1) },
	})
	stream.AddAnyHook(action.AnyHook{
		OnError:  func(context.Context, any, error, *action.Meta) { anyErrors.Add(1) },
		OnCancel: func(context.Context, any, *action.Meta) { anyCancel.Add(1) },
	})
	seq, err := stream.Do(ctx, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		var terminalError error
		for _, itemError := range seq {
			if itemError != nil {
				terminalError = itemError
			}
		}
		done <- terminalError
	}()
	select {
	case <-yielded:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not yield its partial result")
	}
	cancel()
	select {
	case terminalError := <-done:
		if !errors.Is(terminalError, context.Canceled) {
			t.Fatalf("terminal error = %v, want context.Canceled", terminalError)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not terminate after cancellation")
	}
	if success.Load() != 0 || errorsSeen.Load() != 0 || cancelSeen.Load() != 1 {
		t.Fatalf("success=%d stream errors=%d cancel=%d", success.Load(), errorsSeen.Load(), cancelSeen.Load())
	}
	if typedErrors.Load() != 0 || typedCancel.Load() != 1 || anyErrors.Load() != 0 || anyCancel.Load() != 1 {
		t.Fatalf("typed errors=%d cancel=%d; any errors=%d cancel=%d", typedErrors.Load(), typedCancel.Load(), anyErrors.Load(), anyCancel.Load())
	}
	if len(items) != 2 || items[0].value != 1 || items[0].err != nil || items[1].value != 0 || !errors.Is(items[1].err, context.Canceled) {
		t.Fatalf("OnItem pairs = %+v, want [(1, nil), (0, context.Canceled)]", items)
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
			wantErrors := int64(1)
			if name == "cancel" {
				wantErrors = 0
			}
			if errorsSeen.Load() != wantErrors {
				t.Fatalf("OnError=%d, want %d", errorsSeen.Load(), wantErrors)
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

func TestStreamHookEarlyStopReportsSuccessfulItemBeforeTerminalHooks(t *testing.T) {
	var order []string
	stream := action.NewStream("early.stop.success", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			if !yield(7, nil) {
				return
			}
			yield(8, nil)
		}, nil
	}).UseStream(action.StreamHook[struct{}, int]{
		OnItem: func(_ context.Context, _ struct{}, item int, itemErr error, _ *action.Meta) {
			if item != 7 || itemErr != nil {
				t.Errorf("OnItem got (%d, %v), want (7, nil)", item, itemErr)
			}
			order = append(order, "item")
		},
		OnSuccess: func(context.Context, struct{}, *action.Meta) { order = append(order, "success") },
		OnError:   func(context.Context, struct{}, error, *action.Meta) { order = append(order, "error") },
		After:     func(context.Context, struct{}, error, *action.Meta) { order = append(order, "after") },
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	seq(func(item int, itemErr error) bool {
		if item != 7 || itemErr != nil {
			t.Errorf("consumer got (%d, %v), want (7, nil)", item, itemErr)
		}
		order = append(order, "yield")
		return false
	})
	if want := []string{"yield", "item", "success", "after"}; !slices.Equal(order, want) {
		t.Fatalf("callback order = %v, want %v", order, want)
	}
}

func TestStreamHookEarlyStopReportsErrorItemBeforeTerminalOnError(t *testing.T) {
	itemErr := errors.New("item failed")
	var order []string
	stream := action.NewStream("early.stop.error", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			if !yield(7, itemErr) {
				return
			}
			yield(8, nil)
		}, nil
	}).UseStream(action.StreamHook[struct{}, int]{
		OnItem: func(_ context.Context, _ struct{}, item int, gotErr error, _ *action.Meta) {
			if item != 7 || !errors.Is(gotErr, itemErr) {
				t.Errorf("OnItem got (%d, %v), want (7, %v)", item, gotErr, itemErr)
			}
			order = append(order, "item")
		},
		OnSuccess: func(context.Context, struct{}, *action.Meta) { order = append(order, "success") },
		OnError: func(_ context.Context, _ struct{}, terminalErr error, _ *action.Meta) {
			if !errors.Is(terminalErr, itemErr) {
				t.Errorf("OnError got %v, want %v", terminalErr, itemErr)
			}
			order = append(order, "error")
		},
		After: func(_ context.Context, _ struct{}, terminalErr error, _ *action.Meta) {
			if !errors.Is(terminalErr, itemErr) {
				t.Errorf("After got %v, want %v", terminalErr, itemErr)
			}
			order = append(order, "after")
		},
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	seq(func(item int, gotErr error) bool {
		if item != 7 || !errors.Is(gotErr, itemErr) {
			t.Errorf("consumer got (%d, %v), want (7, %v)", item, gotErr, itemErr)
		}
		order = append(order, "yield")
		return false
	})
	if want := []string{"yield", "item", "error", "after"}; !slices.Equal(order, want) {
		t.Fatalf("callback order = %v, want %v", order, want)
	}
}
