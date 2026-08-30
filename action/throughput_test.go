package action_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
)

func BenchmarkTypedActionDo(b *testing.B) {
	act := action.New("bench.identity", func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	}).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := act.Do(ctx, i)
		if err != nil || value != i+1 {
			b.Fatalf("unexpected result: value=%d err=%v", value, err)
		}
	}
}

func BenchmarkTypedActionDoParallel(b *testing.B) {
	act := action.New("bench.parallel_identity", func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	}).Build()
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := act.Do(ctx, 1)
			if err != nil {
				b.Errorf("unexpected error: %v", err)
				return
			}
		}
	})
}

func BenchmarkActionFanOut(b *testing.B) {
	act := action.New("bench.fanout", func(_ context.Context, value int) (int, error) {
		return value * 2, nil
	}).Build()
	requests := make([]int, 64)
	for i := range requests {
		requests[i] = i
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results := action.FanOut(ctx, act, requests, 8)
		if len(results) != len(requests) {
			b.Fatalf("got %d results, want %d", len(results), len(requests))
		}
		for _, result := range results {
			if result.Err != nil {
				b.Fatal(result.Err)
			}
		}
	}
}

func TestTypedActionConcurrentReuse(t *testing.T) {
	const workers = 128
	const callsPerWorker = 500
	var calls atomic.Int64

	act := action.New("test.concurrent_reuse", func(_ context.Context, value int) (int, error) {
		calls.Add(1)
		return value + 1, nil
	}).Build()

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for call := 0; call < callsPerWorker; call++ {
				got, err := act.Do(ctx, worker+call)
				if err != nil {
					t.Errorf("worker %d: %v", worker, err)
					return
				}
				if got != worker+call+1 {
					t.Errorf("worker %d: got %d", worker, got)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	if got, want := calls.Load(), int64(workers*callsPerWorker); got != want {
		t.Fatalf("calls=%d, want %d", got, want)
	}
}

func TestFanOutConcurrentSafety(t *testing.T) {
	const count = 256
	var active atomic.Int64
	var peak atomic.Int64
	act := action.New("test.fanout_safety", func(_ context.Context, value int) (int, error) {
		current := active.Add(1)
		for {
			old := peak.Load()
			if current <= old || peak.CompareAndSwap(old, current) {
				break
			}
		}
		active.Add(-1)
		return value, nil
	}).Build()

	requests := make([]int, count)
	for i := range requests {
		requests[i] = i
	}
	results := action.FanOut(context.Background(), act, requests, 16)
	if len(results) != count {
		t.Fatalf("results=%d, want %d", len(results), count)
	}
	for i, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d: %v", i, result.Err)
		}
		if result.Value != i {
			t.Fatalf("result %d: value=%d, want %d", i, result.Value, i)
		}
	}
	if peak.Load() > 16 {
		t.Fatalf("peak concurrency=%d, want <=16", peak.Load())
	}
}
