package action_test

import (
	"context"
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

	stream := action.NewStream("test.init.panic", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		panic("panic_during_init")
	}).Use(action.Hook[struct{}, iter.Seq2[int, error]]{
		After: func(_ context.Context, _ struct{}, _ iter.Seq2[int, error], err error, _ *action.Meta) {
			if err != nil {
				afterHookRan.Store(true)
			}
		},
	})

	_, err := stream.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected Do() to return error on panic, got nil")
	}
	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal, got: %v", err)
	}
	if !afterHookRan.Load() {
		t.Fatal("After hook was not executed on init panic")
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
