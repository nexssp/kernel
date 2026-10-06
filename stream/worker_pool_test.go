package stream_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/stream"
	"github.com/nexssp/kernel/xtest"
)

// ── happy path ─────────────────────────────────────────────────────────

func TestWorkerPool_HappyPath(t *testing.T) {
	t.Parallel()

	op := stream.WorkerPool[int, int](context.Background(), 3, 0,
		func(_ context.Context, n int) (int, error) {
			return n * 2, nil
		})

	items, errs := drain(op(source(1, 2, 3, 4, 5)))

	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}

	sort.Ints(items)
	want := []int{2, 4, 6, 8, 10}
	for i, v := range want {
		if items[i] != v {
			t.Fatalf("items[%d] = %d, want %d (all: %v)", i, items[i], v, items)
		}
	}
}

func TestWorkerPool_EmptyInput(t *testing.T) {
	t.Parallel()

	op := stream.WorkerPool[int, int](context.Background(), 4, 0,
		func(_ context.Context, n int) (int, error) { return n, nil })

	items, errs := drain(op(source[int]()))

	if len(items) != 0 || len(errs) != 0 {
		t.Fatalf("expected empty, got items=%v errs=%v", items, errs)
	}
}

func TestWorkerPool_NilWorkerYieldsError(t *testing.T) {
	t.Parallel()

	op := stream.WorkerPool[int, int](context.Background(), 2, 0, nil)

	_, errs := drain(op(source(1)))

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %v", errs)
	}
	if errs[0] == nil {
		t.Fatal("expected non-nil error")
	}
}

// ── concurrency bound ──────────────────────────────────────────────────

func TestWorkerPool_ConcurrencyIsBounded(t *testing.T) {
	t.Parallel()

	var (
		inFlight atomic.Int64
		peak     atomic.Int64
	)

	op := stream.WorkerPool[int, int](context.Background(), 3, 0,
		func(_ context.Context, n int) (int, error) {
			cur := inFlight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			inFlight.Add(-1)
			return n, nil
		})

	items := make([]int, 30)
	for i := range items {
		items[i] = i
	}

	out, errs := drain(op(source(items...)))
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(out) != 30 {
		t.Fatalf("expected 30 items, got %d", len(out))
	}
	if got := peak.Load(); got > 3 {
		t.Fatalf("concurrency exceeded: peak=%d", got)
	}
}

// ── error propagation ──────────────────────────────────────────────────

func TestWorkerPool_WorkerErrorIsPerItem(t *testing.T) {
	t.Parallel()

	op := stream.WorkerPool[int, int](context.Background(), 4, 0,
		func(_ context.Context, n int) (int, error) {
			if n == 3 {
				return 0, fmt.Errorf("item %d failed", n)
			}
			return n, nil
		})

	items, errs := drain(op(source(1, 2, 3, 4, 5)))

	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %v", errs)
	}
	if len(items) != 4 {
		t.Fatalf("expected 4 successful items, got %d: %v", len(items), items)
	}
}

func TestWorkerPool_UpstreamErrorPropagates(t *testing.T) {
	t.Parallel()

	upstream := func(yield func(int, error) bool) {
		yield(1, nil)
		yield(2, nil)
		yield(0, errors.New("upstream boom"))
	}

	op := stream.WorkerPool[int, int](context.Background(), 2, 0,
		func(_ context.Context, n int) (int, error) { return n, nil })

	_, errs := drain(op(upstream))

	if len(errs) != 1 {
		t.Fatalf("expected 1 upstream error, got %v", errs)
	}
	if errs[0] == nil || errs[0].Error() != "upstream boom" {
		t.Fatalf("unexpected error: %v", errs[0])
	}
}

// ── panic isolation ────────────────────────────────────────────────────

func TestWorkerPool_WorkerPanicIsIsolated(t *testing.T) {
	t.Parallel()

	op := stream.WorkerPool[int, int](context.Background(), 4, 0,
		func(_ context.Context, n int) (int, error) {
			if n == 3 {
				panic("worker kaboom")
			}
			return n, nil
		})

	items, errs := drain(op(source(1, 2, 3, 4, 5)))

	if len(errs) != 1 {
		t.Fatalf("expected 1 panic error, got %v", errs)
	}
	if len(items) != 4 {
		t.Fatalf("expected 4 successful items, got %d: %v", len(items), items)
	}
	if !strings.Contains(errs[0].Error(), "worker panicked") {
		t.Fatalf("unexpected error message: %v", errs[0])
	}
}

func TestWorkerPool_UpstreamPanicIsIsolated(t *testing.T) {
	t.Parallel()

	upstream := func(yield func(int, error) bool) {
		yield(1, nil)
		panic("upstream kaboom")
	}

	op := stream.WorkerPool[int, int](context.Background(), 2, 0,
		func(_ context.Context, n int) (int, error) { return n, nil })

	_, errs := drain(op(upstream))

	if len(errs) != 1 {
		t.Fatalf("expected 1 upstream panic error, got %v", errs)
	}
	if !strings.Contains(errs[0].Error(), "upstream panicked") {
		t.Fatalf("unexpected error message: %v", errs[0])
	}
}

// ── cancellation ───────────────────────────────────────────────────────

// TestWorkerPool_ContextCanceledDuringWork asserts the real contract:
// when the caller cancels ctx, the operator terminates promptly. Exact
// error counts depend on scheduler timing and are not asserted.
func TestWorkerPool_ContextCanceledDuringWork(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	op := stream.WorkerPool[int, int](ctx, 2, 0,
		func(workerCtx context.Context, n int) (int, error) {
			if n == 2 {
				cancel()
			}
			<-workerCtx.Done()
			return 0, workerCtx.Err()
		})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = drain(op(source(1, 2, 3, 4, 5)))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("operator did not terminate after context cancel")
	}
}

// TestWorkerPool_DownstreamEarlyStop asserts that stopping the consumer
// halts the pool before the slow feeder has produced its entire stream.
// The feeder sleeps 1ms per item, so the full 1000 items need ≥ 1s;
// the test's break-out path is microseconds, leaving a wide margin.
func TestWorkerPool_DownstreamEarlyStop(t *testing.T) {
	t.Parallel()

	const total = 1000

	var processed atomic.Int64

	op := stream.WorkerPool[int, int](context.Background(), 2, 0,
		func(_ context.Context, n int) (int, error) {
			processed.Add(1)
			return n, nil
		})

	feed := func(yield func(int, error) bool) {
		for i := 1; i <= total; i++ {
			if !yield(i, nil) {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}

	consumed := 0
	for range op(feed) {
		consumed++
		if consumed == 2 {
			break
		}
	}

	if got := processed.Load(); got >= 100 {
		t.Fatalf("early stop failed: processed=%d of %d", got, total)
	}
}

// ── goroutine leak ─────────────────────────────────────────────────────

// TestWorkerPool_NoGoroutineLeak verifies that all pool goroutines exit
// after the stream drains. Not parallel: runtime.NumGoroutine is
// process-global and RequireNoGoroutineLeak documents this restriction.
func TestWorkerPool_NoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	op := stream.WorkerPool[int, int](context.Background(), 3, 0,
		func(_ context.Context, n int) (int, error) { return n, nil })

	items, errs := drain(op(source(1, 2, 3, 4, 5)))
	if len(errs) != 0 || len(items) != 5 {
		t.Fatalf("unexpected: items=%v errs=%v", items, errs)
	}

	xtest.RequireNoGoroutineLeak(t, baseline, time.Second)
}

// TestWorkerPool_DeadlockOnCanceledContextWhenUpstreamBlocks dowodzi,
// że drainResults zawiesza się na sztywnym `<-feedDone`, gdy kontekst zostanie anulowany,
// a upstream jest zablokowany na operacji I/O.
func TestWorkerPool_DeadlockOnCanceledContextWhenUpstreamBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unblockUpstream := make(chan struct{})
	defer close(unblockUpstream)

	// Upstream, który emituje 1 element, a potem symuluje oczekiwanie na wolne I/O
	slowUpstream := func(yield func(int, error) bool) {
		if !yield(1, nil) {
			return
		}
		// Symulacja blokady (np. socket/kolejka bez nowych wiadomości)
		<-unblockUpstream
	}

	op := stream.WorkerPool[int, int](ctx, 2, 0, func(_ context.Context, n int) (int, error) {
		return n, nil
	})

	seq := op(slowUpstream)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, err := range seq {
			if err != nil {
				return
			}
			// Po odebraniu pierwszego elementu anulujemy kontekst całego strumienia!
			cancel()
		}
	}()

	// DrainResults hangs on <-feedDone when context is canceled!
	select {
	case <-done:
		// Sukces: zamknięcie nastąpiło szybko
	case <-time.After(500 * time.Millisecond):
		t.Fatal("DEADLOCK / GOROUTINE HANG: WorkerPool zablokował się na <-feedDone mimo anulowania contextu!")
	}
}
