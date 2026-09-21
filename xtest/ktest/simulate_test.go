package ktest_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestSimulate_ThunderingHerdBarrier(t *testing.T) {
	t.Parallel()

	var activeWorkers, peakConcurrency, totalExecutions atomic.Int32

	act := action.New("barrier.test", func(_ context.Context, req int) (int, error) {
		totalExecutions.Add(1)
		cur := activeWorkers.Add(1)

		for {
			old := peakConcurrency.Load()
			if cur <= old || peakConcurrency.CompareAndSwap(old, cur) {
				break
			}
		}

		time.Sleep(10 * time.Millisecond)
		activeWorkers.Add(-1)
		return req * 2, nil
	}).Build()

	const workers = 50

	ktest.Simulate(t, act, 10, workers, func(t testing.TB, res int, err error) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if res != 20 {
			t.Errorf("expected 20, got %d", res)
		}
	})

	if totalExecutions.Load() != workers {
		t.Fatalf("executions = %d, want %d", totalExecutions.Load(), workers)
	}
	if peakConcurrency.Load() < 10 {
		t.Fatalf("barrier failed: peak concurrency was %d (want >= 10)", peakConcurrency.Load())
	}
}

func TestSimulate_RejectsZeroConcurrency(t *testing.T) {
	xtest.ExpectFatal(t, "TestSimulate_RejectsZeroConcurrency", "XTEST_SIMULATE_ZERO",
		"concurrency must be > 0",
		func(t *testing.T) {
			act := action.New("x", func(_ context.Context, _ struct{}) (struct{}, error) {
				return struct{}{}, nil
			}).Build()
			ktest.Simulate(t, act, struct{}{}, 0, func(testing.TB, struct{}, error) {})
		})
}
