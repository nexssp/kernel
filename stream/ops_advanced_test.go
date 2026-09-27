// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/stream"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestWindowOp(t *testing.T) {
	ctx, _ := ktest.Ctx(t)
	src := ktest.StreamOf("src", 1, 2, 3, 4, 5)

	// Window of size 3, step 2.
	// Expected: [1,2,3], [3,4,5], [5] (final partial)
	winAct := action.NewStreamOp("win", src, stream.Window[int](3, 2))
	res, err := action.CollectStream(ctx, winAct, struct{}{})
	ktest.RequireNoError(t, err)

	ktest.RequireEqual(t, len(res), 3)
	ktest.RequireEqual(t, res[0], []int{1, 2, 3})
	ktest.RequireEqual(t, res[1], []int{3, 4, 5})
	ktest.RequireEqual(t, res[2], []int{5})
}

func TestBatchByTimeOp(t *testing.T) {
	ctx, _ := ktest.Ctx(t)
	src := ktest.StreamOf("src", 1, 2, 3, 4, 5)

	// We pass a very large timeout so the batch flushes only on count (size=2)
	// and on EOF (flushes the remaining items).
	batchAct := action.NewStreamOp("batch", src, stream.BatchByTime[int](ctx, 2, 1*time.Hour))
	res, err := action.CollectStream(ctx, batchAct, struct{}{})
	ktest.RequireNoError(t, err)

	ktest.RequireEqual(t, len(res), 3)
	ktest.RequireEqual(t, res[0], []int{1, 2})
	ktest.RequireEqual(t, res[1], []int{3, 4})
	ktest.RequireEqual(t, res[2], []int{5})
}

func TestBatchByTimeOp_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	src := ktest.StreamOf("src", 1, 2, 3)
	batchAct := action.NewStreamOp("batch", src, stream.BatchByTime[int](ctx, 5, 1*time.Hour))

	// Cancel before collection can finish its block
	cancel()

	_, err := action.CollectStream(ctx, batchAct, struct{}{})
	ktest.RequireErrorIs(t, err, context.Canceled)
}
