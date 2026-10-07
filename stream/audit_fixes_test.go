// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Regression tests for stream audit fixes: WorkerPool feedDone-before-
// close ordering, and partial batch flush before terminal error.

package stream_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nexssp/kernel/stream"
)

// UPSTREAM ERROR DELIVERY UNDER FAST DRAIN: the feeder must not lose the
// terminal upstream error when workers exit and resultChan closes before the
// feeder's send would have been observed. Runs many iterations because the
// original bug was a scheduling race.
func TestWorkerPool_UpstreamError_AlwaysDelivered(t *testing.T) {
	t.Parallel()

	boom := errors.New("upstream exploded")
	const iterations = 300

	for i := range iterations {
		upstream := func(yield func(int, error) bool) {
			yield(1, nil)
			yield(2, nil)
			yield(0, boom)
		}

		op := stream.WorkerPool(context.Background(), 4, 2, func(_ context.Context, n int) (int, error) {
			return n, nil
		})

		var gotErr error
		for _, err := range op(upstream) {
			if err != nil {
				gotErr = err
			}
		}
		if !errors.Is(gotErr, boom) {
			t.Fatalf("iteration %d: upstream error lost (got %v)", i, gotErr)
		}
	}
}

// UPSTREAM PANIC DELIVERY: same contract for panicking iterators.
func TestWorkerPool_UpstreamPanic_AlwaysDelivered(t *testing.T) {
	t.Parallel()

	const iterations = 100
	for i := range iterations {
		upstream := func(yield func(int, error) bool) {
			yield(1, nil)
			panic(fmt.Sprintf("iter-%d boom", i))
		}

		op := stream.WorkerPool(context.Background(), 2, 2, func(_ context.Context, n int) (int, error) {
			return n, nil
		})

		var gotErr error
		for _, err := range op(upstream) {
			if err != nil {
				gotErr = err
			}
		}
		if gotErr == nil {
			t.Fatalf("iteration %d: upstream panic lost", i)
		}
	}
}

// PARTIAL BATCH SURVIVES THE TERMINAL ERROR: items received before the
// upstream error must be delivered before the error, not dropped.
func TestBatch_FlushesPartialBatchBeforeError(t *testing.T) {
	t.Parallel()

	boom := errors.New("late failure")
	upstream := func(yield func(int, error) bool) {
		yield(1, nil)
		yield(2, nil)
		yield(0, boom)
	}

	op := stream.Batch[int](5)
	var gotBatch []int
	var gotErr error
	for batch, err := range op(upstream) {
		if err != nil {
			gotErr = err
			continue
		}
		gotBatch = batch
	}

	if !errors.Is(gotErr, boom) {
		t.Fatalf("expected terminal error, got: %v", gotErr)
	}
	if len(gotBatch) != 2 || gotBatch[0] != 1 || gotBatch[1] != 2 {
		t.Fatalf("partial batch dropped: %v", gotBatch)
	}
}

// BatchByTime: partial batch flushed on upstream error.
func TestBatchByTime_FlushesPartialBatchBeforeError(t *testing.T) {
	t.Parallel()

	boom := errors.New("late failure")
	upstream := func(yield func(int, error) bool) {
		yield(1, nil)
		yield(0, boom)
	}

	op := stream.BatchByTime[int](context.Background(), 8, 500*time.Millisecond)
	var gotBatch []int
	var gotErr error
	for batch, err := range op(upstream) {
		if err != nil {
			gotErr = err
			continue
		}
		gotBatch = batch
	}

	if !errors.Is(gotErr, boom) {
		t.Fatalf("expected terminal error, got: %v", gotErr)
	}
	if len(gotBatch) != 1 || gotBatch[0] != 1 {
		t.Fatalf("partial batch dropped: %v", gotBatch)
	}
}
