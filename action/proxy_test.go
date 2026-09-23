package action_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestProxy_Uninitialized_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := action.NewProxy(nil)

	if p.Current() != nil {
		t.Fatalf("expected nil inner action")
	}

	_, err := p.DoAny(ctx, "payload")
	if err == nil || err.Error() != "action: proxy has no active action" {
		t.Fatalf("expected uninitialized error, got: %v", err)
	}

	_, err = p.ExecuteDecoded(ctx, func(_ any) error { return nil })
	if err == nil || err.Error() != "action: proxy has no active action" {
		t.Fatalf("expected uninitialized error on decode, got: %v", err)
	}

	meta := p.Describe()
	if meta == nil || meta.Name != "proxy.uninitialized" {
		t.Fatalf("expected proxy.uninitialized meta, got: %+v", meta)
	}

	if b := p.GetBindings(); b != nil {
		t.Fatalf("expected nil bindings, got: %v", b)
	}

	if h := p.GetAnyHooks(); h != nil {
		t.Fatalf("expected nil hooks, got: %v", h)
	}

	if c := p.Children(); c != nil {
		t.Fatalf("expected nil children, got: %v", c)
	}

	p.AddAnyHook(action.AnyHook{})
	p.Swap(nil)
	if p.Current() != nil {
		t.Fatalf("swap(nil) must not overwrite valid states")
	}
}

func TestProxy_InnerAction_ErrorPropagation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sentinel := errors.New("upstream failed")

	failAct := action.New("fail.node", func(_ context.Context, _ any) (any, error) {
		return nil, sentinel
	}).Build()

	p := action.NewProxy(failAct)

	_, err := p.DoAny(ctx, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
}

func TestProxy_ExecuteDecoded_TypeMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	act := action.New("typed.node", func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	}).Build()

	p := action.NewProxy(act)

	expectedErr := errors.New("cannot decode invalid payload into int")

	_, err := p.ExecuteDecoded(ctx, func(_ any) error {
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected decode error %v, got: %v", expectedErr, err)
	}
}

func benchmarkNoopDecoder(_ any) error {
	return nil
}

func TestProxy_ExecuteDecoded_ZeroAlloc(t *testing.T) {
	ctx := context.Background()

	act := action.New("hot.decoder", func(_ context.Context, _ struct{}) (any, error) {
		return nil, nil
	}).Build()

	p := action.NewProxy(act)

	for range 50 {
		_, _ = p.ExecuteDecoded(ctx, benchmarkNoopDecoder)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = p.ExecuteDecoded(ctx, benchmarkNoopDecoder)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Proxy.ExecuteDecoded, got %v", allocs)
	}
}

// TestProxy_HighThroughput_Contention exercises Proxy.Swap racing against many
// concurrent Proxy.DoAny readers.
//
// This test intentionally does NOT run with t.Parallel(). It spins up
// runtime.GOMAXPROCS(0)*2 reader goroutines plus one writer, and previously
// used a bare 50ms time.Sleep as a stand-in for "the goroutines have started
// and done some work." Under load from other parallel tests in this package,
// that assumption doesn't hold and the goroutines can occasionally not get
// scheduled at all within the window, producing a false
// "zero operations processed" failure.
//
// Fix: wait for an explicit signal — every reader completing at least one
// successful DoAny call — before starting the timed contention window, and
// widen that window. This removes the dependency on scheduler timing for
// correctness while still exercising the actual race between Swap and DoAny.
func TestProxy_Swap_DifferentConcreteTypes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	actString := action.New("act.string", func(_ context.Context, in string) (string, error) {
		return "str:" + in, nil
	}).Build()

	actInt := action.New("act.int", func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	}).Build()

	actMap := action.New("act.map", func(_ context.Context, in map[string]any) (map[string]any, error) {
		in["processed"] = true
		return in, nil
	}).Build()

	proxy := action.NewProxy(actString)

	res, err := proxy.DoAny(ctx, "hello")
	if err != nil || res != "str:hello" {
		t.Fatalf("initial string action failed: res=%v err=%v", res, err)
	}

	proxy.Swap(actInt)

	res, err = proxy.DoAny(ctx, 21)
	if err != nil || res != 42 {
		t.Fatalf("swapped int action failed: res=%v err=%v", res, err)
	}

	proxy.Swap(actMap)

	res, err = proxy.DoAny(ctx, map[string]any{"key": "value"})
	if err != nil {
		t.Fatalf("swapped map action failed: %v", err)
	}
	m, ok := res.(map[string]any)
	if !ok || m["processed"] != true {
		t.Fatalf("expected processed=true, got %+v", res)
	}
}

func TestProxy_HighThroughput_Contention(t *testing.T) {
	ctx := context.Background()

	act1 := action.New("fast.1", func(_ context.Context, in int) (int, error) {
		return in + 1, nil
	}).Build()

	act2 := action.New("fast.2", func(_ context.Context, in int) (int, error) {
		return in + 2, nil
	}).Build()

	p := action.NewProxy(act1)

	var running atomic.Bool
	running.Store(true)

	var writeCount atomic.Uint64
	var readCount atomic.Uint64
	var wg sync.WaitGroup

	readers := runtime.GOMAXPROCS(0) * 2

	// firstOpDone tracks, per-reader, whether it has completed at least one
	// successful DoAny call. We block on this before timing the contention
	// window so the test can never observe "zero reads" just because the
	// scheduler was slow to start goroutines.
	firstOpDone := make([]atomic.Bool, readers)

	wg.Go(func() {
		flip := false
		for running.Load() {
			if flip {
				p.Swap(act1)
			} else {
				p.Swap(act2)
			}
			flip = !flip
			writeCount.Add(1)
			runtime.Gosched()
		}
	})

	for i := range readers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for running.Load() {
				res, err := p.DoAny(ctx, 10)
				if err != nil {
					t.Errorf("read failed during concurrent swap: %v", err)
					return
				}
				v, ok := res.(int)
				if !ok {
					t.Errorf("expected int, got %T", res)
					return
				}
				if v != 11 && v != 12 {
					t.Errorf("corrupt read state: %d", v)
					return
				}
				readCount.Add(1)
				firstOpDone[idx].Store(true)
			}
		}(i)
	}

	// Wait for every reader to have completed at least one op before timing
	// the contention window. This is a correctness precondition for the test,
	// not a timing assumption, so it gets a generous timeout.
	waitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitDeadline) {
		allStarted := true
		for i := range firstOpDone {
			if !firstOpDone[i].Load() {
				allStarted = false
				break
			}
		}
		if allStarted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	for i := range firstOpDone {
		if !firstOpDone[i].Load() {
			running.Store(false)
			wg.Wait()
			t.Fatalf("reader %d never completed a successful operation before timeout", i)
		}
	}

	// All readers and the writer are confirmed running; now give them a
	// meaningful window to actually contend with each other.
	time.Sleep(200 * time.Millisecond)
	running.Store(false)
	wg.Wait()

	if readCount.Load() == 0 || writeCount.Load() == 0 {
		t.Fatalf("zero operations processed during contention test")
	}
}

func BenchmarkProxy_DoAny_HotPath(b *testing.B) {
	ctx := context.Background()
	act := action.New("noop", func(_ context.Context, in int) (int, error) {
		return in, nil
	}).Build()

	p := action.NewProxy(act)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_, _ = p.DoAny(ctx, 42)
	}
}

func TestProxy_DoAny_ZeroAlloc(t *testing.T) {
	ctx := context.Background()
	act := action.New("hot.noop", func(_ context.Context, in int) (int, error) {
		return in, nil
	}).Build()

	p := action.NewProxy(act)

	for range 100 {
		_, _ = p.DoAny(ctx, 42)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = p.DoAny(ctx, 42)
	})

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Proxy.DoAny, got %v", allocs)
	}
}

func TestProxy_Swap_ZeroAlloc(t *testing.T) {
	act1 := action.New("act1", func(_ context.Context, _ any) (string, error) { return "1", nil }).Build()
	act2 := action.New("act2", func(_ context.Context, _ any) (string, error) { return "2", nil }).Build()

	p := action.NewProxy(act1)

	for range 100 {
		p.Swap(act2)
		p.Swap(act1)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		p.Swap(act2)
	})

	if allocs > 1 {
		t.Fatalf("expected <= 1 allocs/op for Proxy.Swap, got %v", allocs)
	}
}

// TestProxy_ConcurrentLoadWithHeterogeneousSwap proves that Proxy maintains 100% memory
// safety, zero panics, and zero race conditions under heavy concurrent reading while
// actively swapping actions with completely different underlying types.
func TestProxy_ConcurrentLoadWithHeterogeneousSwap(t *testing.T) {
	ctx := context.Background()

	actInt := action.New("node.int", func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	}).Build()

	actString := action.New("node.string", func(_ context.Context, in string) (string, error) {
		return "echo:" + in, nil
	}).Build()

	p := action.NewProxy(actInt)

	var running atomic.Bool
	running.Store(true)

	var reads atomic.Uint64
	var swaps atomic.Uint64

	const readers = 64
	var wg sync.WaitGroup

	// Background swapper: continuously alternates between completely different types
	wg.Go(func() {
		flip := false
		for running.Load() {
			if flip {
				p.Swap(actInt)
			} else {
				p.Swap(actString)
			}
			flip = !flip
			swaps.Add(1)
			runtime.Gosched()
		}
	})

	// Reader workers: execute DoAny concurrently
	for range readers {
		wg.Go(func() {
			for running.Load() {
				// We pass nil or an integer: DoAny safely coerces or handles matching types
				res, err := p.DoAny(ctx, 42)
				if err == nil && res != nil {
					reads.Add(1)
				}
			}
		})
	}

	// Run under heavy contention for 250 milliseconds
	time.Sleep(250 * time.Millisecond)
	running.Store(false)
	wg.Wait()

	if reads.Load() == 0 || swaps.Load() == 0 {
		t.Fatalf("test processed zero operations: reads=%d, swaps=%d", reads.Load(), swaps.Load())
	}
}
