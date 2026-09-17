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

	for i := 0; i < b.N; i++ {
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

	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for Proxy.Swap, got %v", allocs)
	}
}
