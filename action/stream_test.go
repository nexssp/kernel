package action_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestStream_HappyPath(t *testing.T) {
	t.Parallel()

	var beforeCalled, afterCalled atomic.Bool

	stream := action.NewStream("test.stream", func(_ context.Context, count int) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			for i := 1; i <= count; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}, nil
	}).Use(action.Hook[int, iter.Seq2[int, error]]{
		Before: func(ctx context.Context, _ int, _ *action.Meta) (context.Context, error) {
			beforeCalled.Store(true)
			return ctx, nil
		},
		After: func(_ context.Context, _ int, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			if err != nil {
				t.Errorf("unexpected error in After hook: %v", err)
			}
			afterCalled.Store(true)
		},
	})

	seq, err := stream.Do(context.Background(), 3)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	var collected []int
	for item, err := range seq {
		if err != nil {
			t.Fatalf("unexpected item error: %v", err)
		}
		collected = append(collected, item)
	}

	if len(collected) != 3 || collected[2] != 3 {
		t.Fatalf("unexpected collected values: %v", collected)
	}
	if !beforeCalled.Load() || !afterCalled.Load() {
		t.Fatal("expected both Before and After hooks to execute")
	}
}

func TestStream_NilSequenceReturnsControlledError(t *testing.T) {
	stream := action.NewStream("nil.sequence", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return nil, nil
	})

	seq, err := stream.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("Do error = nil, want controlled nil-sequence error")
	}
	if got, want := err.Error(), "action: stream handler returned nil sequence"; got != want {
		t.Fatalf("Do error = %q, want %q", got, want)
	}
	if seq == nil {
		t.Fatal("Do returned nil error sequence")
	}

	count := 0
	var yieldedErr error
	for item, itemErr := range seq {
		count++
		if item != 0 {
			t.Fatalf("error item = %d, want zero value", item)
		}
		yieldedErr = itemErr
	}
	if count != 1 || !errors.Is(yieldedErr, err) {
		t.Fatalf("error sequence yielded %d items with error %v; want one pair with %v", count, yieldedErr, err)
	}
}

func TestStream_AnyStreamActionBoundary(t *testing.T) {
	t.Parallel()

	stream := action.NewStream("search.documents", func(_ context.Context, query string) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			for i := range 3 {
				if !yield(len(query)+i, nil) {
					return
				}
			}
		}, nil
	}).Route("GET /search")

	var mounted action.AnyStreamAction = stream
	if mounted.Describe().Name != "search.documents" {
		t.Fatalf("unexpected stream name: %q", mounted.Describe().Name)
	}
	if len(mounted.GetBindings()) != 1 {
		t.Fatalf("bindings = %d, want 1", len(mounted.GetBindings()))
	}
	if got := reflect.TypeOf(mounted.ReqPayload()); got != reflect.TypeFor[string]() {
		t.Fatalf("request payload type = %v, want string", got)
	}
	if got := reflect.TypeOf(mounted.ResPayload()); got != reflect.TypeFor[int]() {
		t.Fatalf("item payload type = %v, want int", got)
	}

	anyStream, err := mounted.DoStreamAny(context.Background(), "go")
	if err != nil {
		t.Fatalf("DoStreamAny failed: %v", err)
	}
	var got []any
	anyStream(func(item any, err error) bool {
		if err != nil {
			t.Errorf("unexpected item error: %v", err)
			return false
		}
		got = append(got, item)
		return true
	})
	if len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("items = %v, want [2 3 4]", got)
	}
}

func TestStream_DoStreamTypedKeepsItemType(t *testing.T) {
	t.Parallel()

	type request struct {
		Prefix string `json:"prefix"`
	}
	source := action.NewStream("typed.stream", func(_ context.Context, req request) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			if !yield(len(req.Prefix), nil) {
				return
			}
			yield(len(req.Prefix)+1, nil)
		}, nil
	})

	seq, err := source.DoStreamTyped(context.Background(), map[string]any{"prefix": "go"})
	if err != nil {
		t.Fatalf("DoStreamTyped failed: %v", err)
	}
	var got []int
	for item, itemErr := range seq {
		if itemErr != nil {
			t.Fatalf("unexpected item error: %v", itemErr)
		}
		got = append(got, item)
	}
	if !reflect.DeepEqual(got, []int{2, 3}) {
		t.Fatalf("typed items = %v, want [2 3]", got)
	}
}

func TestStream_ConsumerPanic_NotSwallowed(t *testing.T) {
	t.Parallel()

	var afterHookSawError atomic.Bool

	stream := action.NewStream("test.consumer.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			yield(2, nil)
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			if err != nil {
				afterHookSawError.Store(true)
			}
		},
	})

	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	consumerPanicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				if r == "consumer_kaboom" {
					consumerPanicked = true
				}
			}
		}()

		for item := range seq {
			if item == 2 {
				panic("consumer_kaboom")
			}
		}
	}()

	if !consumerPanicked {
		t.Fatal("CRITICAL: consumer panic was silently swallowed by stream wrapper!")
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive error notification about the panic")
	}
}

func TestStream_GeneratorPanic_NotSwallowed(t *testing.T) {
	t.Parallel()

	var afterHookSawError atomic.Bool

	stream := action.NewStream("test.generator.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			panic("generator_internal_crash")
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			if err != nil {
				afterHookSawError.Store(true)
			}
		},
	})

	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	generatorPanicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				if r == "generator_internal_crash" {
					generatorPanicked = true
				}
			}
		}()

		for item := range seq {
			_ = item
		}
	}()

	if !generatorPanicked {
		t.Fatal("CRITICAL: generator panic was silently swallowed!")
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive error notification about generator panic")
	}
}

func TestStream_HandlerPanicInDo(t *testing.T) {
	t.Parallel()

	var afterHookRan atomic.Bool
	var recovered any
	var callbackOrder []string

	stream := action.NewStream("test.init.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		panic("panic_during_init")
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			if err != nil {
				afterHookRan.Store(true)
			}
			callbackOrder = append(callbackOrder, "typed-after")
		},
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, value any, _ *action.Meta) {
			recovered = value
			callbackOrder = append(callbackOrder, "panic")
		},
		OnError: func(_ context.Context, _ any, err error, _ *action.Meta) {
			if err == nil {
				t.Error("OnError received nil for a recovered panic")
			}
			callbackOrder = append(callbackOrder, "error")
		},
		After: func(_ context.Context, _, _ any, err error, _ *action.Meta) {
			if err == nil {
				t.Error("AnyHook.After received nil for a recovered panic")
			}
			callbackOrder = append(callbackOrder, "any-after")
		},
	})

	_, err := stream.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected Do() to return error on panic, got nil")
	}
	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal, got: %v", err)
	}
	if recovered != "panic_during_init" {
		t.Fatalf("OnPanic received %v, want panic_during_init", recovered)
	}
	if !afterHookRan.Load() {
		t.Fatal("After hook was not executed on init panic")
	}
	wantOrder := []string{"panic", "error", "any-after", "typed-after"}
	if !reflect.DeepEqual(callbackOrder, wantOrder) {
		t.Fatalf("callback order = %v, want %v", callbackOrder, wantOrder)
	}
}

func TestCollectStream_GeneratorPanicCaptured(t *testing.T) {
	t.Parallel()

	stream := action.NewStream("test.collect.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(10, nil)
			panic("crash_during_iteration")
		}, nil
	})

	_, err := action.CollectStream(context.Background(), stream, struct{}{})
	if err == nil {
		t.Fatal("expected CollectStream to return error on generator panic")
	}
	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal error, got: %v", err)
	}
}

func TestStream_ConsumerPanicDoesNotFireActionOnPanic(t *testing.T) {
	t.Parallel()

	var panicHookRan atomic.Bool
	stream := action.NewStream("test.consumer.panic.hook", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
		}, nil
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, _ any, _ *action.Meta) {
			panicHookRan.Store(true)
		},
	})

	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	consumerPanicked := false
	func() {
		defer func() {
			consumerPanicked = recover() == "consumer_origin"
		}()
		for range seq {
			panic("consumer_origin")
		}
	}()
	if !consumerPanicked {
		t.Fatal("consumer panic was not re-panicked")
	}
	if panicHookRan.Load() {
		t.Fatal("consumer panic must not be reported as an action panic")
	}
}

func TestStream_SourceIteratorPanicFiresActionOnPanic(t *testing.T) {
	t.Parallel()

	var hookPanicValue any
	var repanickedValue any
	var afterHookSawError atomic.Bool
	stream := action.NewStream("test.source.iterator.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			panic("source_iterator_crash")
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			afterHookSawError.Store(err != nil)
		},
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, recovered any, _ *action.Meta) {
			hookPanicValue = recovered
		},
	})

	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed before iteration: %v", err)
	}
	func() {
		defer func() { repanickedValue = recover() }()
		for range seq {
			continue
		}
	}()
	if hookPanicValue != "source_iterator_crash" {
		t.Fatalf("OnPanic value = %v, want source_iterator_crash", hookPanicValue)
	}
	if repanickedValue != "source_iterator_crash" {
		t.Fatalf("re-panic value = %v, want source_iterator_crash", repanickedValue)
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive the source panic error")
	}
}

func TestStream_SyntheticCancelConsumerPanicDoesNotFireActionOnPanic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var panicHookRan atomic.Bool
	stream := action.NewStream("test.synthetic.cancel.consumer.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(func(int, error) bool) {}, nil
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, _ any, _ *action.Meta) {
			panicHookRan.Store(true)
		},
	})
	seq, err := stream.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	cancel()

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		for _, itemErr := range seq {
			if itemErr != nil {
				panic("synthetic_consumer_crash")
			}
		}
	}()
	if recovered != "synthetic_consumer_crash" {
		t.Fatalf("synthetic yield panic = %v, want consumer panic", recovered)
	}
	if panicHookRan.Load() {
		t.Fatal("synthetic consumer panic must not be reported as an action panic")
	}
}

func TestStream_IteratorRecoveryCannotSwallowConsumerPanic(t *testing.T) {
	t.Parallel()

	var panicHookRan atomic.Bool
	var afterHookSawError atomic.Bool
	var sourceStopped atomic.Bool
	var sourceRecoveredPanic atomic.Bool
	stream := action.NewStream("test.iterator.recovers.consumer.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			defer func() {
				if recover() != nil {
					sourceRecoveredPanic.Store(true)
				}
			}()
			if !yield(1, nil) {
				sourceStopped.Store(true)
			}
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			afterHookSawError.Store(err != nil)
		},
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, _ any, _ *action.Meta) {
			panicHookRan.Store(true)
		},
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		for range seq {
			panic("consumer_origin_recovered_by_iterator")
		}
	}()
	if recovered != "consumer_origin_recovered_by_iterator" {
		t.Fatalf("re-panic value = %v, want original consumer panic", recovered)
	}
	if !sourceStopped.Load() {
		t.Fatal("source iterator did not receive the stop signal")
	}
	if sourceRecoveredPanic.Load() {
		t.Fatal("consumer panic escaped into the source iterator's recovery")
	}
	if panicHookRan.Load() {
		t.Fatal("consumer panic must not be reported as an action panic")
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive an error for the recovered consumer panic")
	}
}

func TestStream_SourcePanicAfterConsumerStopStillFiresActionOnPanic(t *testing.T) {
	t.Parallel()

	var hookPanicValue any
	var repanickedValue any
	var sourceSawStop atomic.Bool
	var afterHookSawError atomic.Bool
	stream := action.NewStream("test.source.panic.after.consumer.stop", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			if !yield(1, nil) {
				sourceSawStop.Store(true)
				panic("source_after_consumer_stop")
			}
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			afterHookSawError.Store(err != nil)
		},
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, recovered any, _ *action.Meta) {
			hookPanicValue = recovered
		},
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	func() {
		defer func() { repanickedValue = recover() }()
		for range seq {
			panic("consumer_before_source_panic")
		}
	}()
	if !sourceSawStop.Load() {
		t.Fatal("source iterator did not receive the stop signal")
	}
	if hookPanicValue != "source_after_consumer_stop" {
		t.Fatalf("OnPanic value = %v, want source_after_consumer_stop", hookPanicValue)
	}
	if repanickedValue != "source_after_consumer_stop" {
		t.Fatalf("re-panic value = %v, want source_after_consumer_stop", repanickedValue)
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive the source panic error")
	}
}

func TestStream_SourcePanicNilRemainsVisible(t *testing.T) {
	t.Parallel()

	var panicHookRan atomic.Bool
	var afterHookSawError atomic.Bool
	stream := action.NewStream("test.source.panic.nil", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(func(int, error) bool) {
			panicWithAny(nil)
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			afterHookSawError.Store(err != nil)
		},
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, _ any, _ *action.Meta) {
			panicHookRan.Store(true)
		},
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	panicked := false
	func() {
		defer func() {
			panicked = true
			_ = recover()
		}()
		for range seq {
			continue
		}
	}()
	if !panicked {
		t.Fatal("source panic(nil) was silently swallowed")
	}
	if !panicHookRan.Load() {
		t.Fatal("OnPanic did not report source panic(nil)")
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive an error for source panic(nil)")
	}
}

func TestStream_ConsumerPanicNilRemainsVisibleWithoutActionOnPanic(t *testing.T) {
	t.Parallel()

	var panicHookRan atomic.Bool
	var afterHookSawError atomic.Bool
	stream := action.NewStream("test.consumer.panic.nil", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
		}, nil
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			afterHookSawError.Store(err != nil)
		},
	})
	stream.AddAnyHook(action.AnyHook{
		OnPanic: func(_ context.Context, _, _ any, _ *action.Meta) {
			panicHookRan.Store(true)
		},
	})
	seq, err := stream.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}

	panicked := false
	func() {
		defer func() {
			panicked = true
			_ = recover()
		}()
		for range seq {
			panicWithAny(nil)
		}
	}()
	if !panicked {
		t.Fatal("consumer panic(nil) was silently swallowed")
	}
	if panicHookRan.Load() {
		t.Fatal("consumer panic(nil) must not be reported as an action panic")
	}
	if !afterHookSawError.Load() {
		t.Fatal("After hook did not receive an error for consumer panic(nil)")
	}
}

func panicWithAny(value any) {
	panic(value)
}
