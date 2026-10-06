package stream

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
)

type workerResult[Out any] struct {
	item Out
	err  error
}

// WorkerPool processes upstream items concurrently using a pool of
// workers. The worker callback runs for each item, up to `concurrency`
// in parallel. Results are yielded as they complete — not in input
// order — so callers that need ordering must sort downstream.
//
// Termination:
//
//   - Upstream ends → feeder closes workChan → workers drain it and
//     exit → resultChan closes after the WaitGroup finishes.
//   - Downstream stops consuming (yield returns false) → cancel pool →
//     workers and feeder observe cancellation and return.
//   - ctx is canceled → same as above.
//
// Panic isolation: a panic inside worker or inside the upstream
// iterator is recovered and delivered as an error on the result stream.
// The pool keeps running other workers; a panicking item produces one
// error and the pool continues with the next item.
//
// The function is safe to call with nil worker (yields a single error)
// and with concurrency < 1 (clamped to 1). bufferCapacity < 1 defaults
// to 2×concurrency.
func WorkerPool[In, Out any](
	ctx context.Context,
	concurrency int,
	bufferCapacity int,
	worker func(context.Context, In) (Out, error),
) StreamOp[In, Out] {
	if concurrency < 1 {
		concurrency = 1
	}
	if bufferCapacity < 1 {
		bufferCapacity = concurrency * 2
	}

	return func(upstream iter.Seq2[In, error]) iter.Seq2[Out, error] {
		return func(yield func(Out, error) bool) {
			if worker == nil {
				var zero Out
				yield(zero, errors.New("stream.WorkerPool: worker function is nil"))
				return
			}

			poolCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			workChan := make(chan In, bufferCapacity)
			resultChan := make(chan workerResult[Out], bufferCapacity)

			var workerGroup sync.WaitGroup
			workerGroup.Add(concurrency)
			for range concurrency {
				go func() {
					defer workerGroup.Done()
					runWorker(poolCtx, worker, workChan, resultChan)
				}()
			}

			feedDone := make(chan error, 1)
			go func() {
				feedDone <- feedUpstream(poolCtx, upstream, workChan)
			}()

			go func() {
				workerGroup.Wait()
				close(resultChan)
			}()

			drainResults(poolCtx, resultChan, feedDone, cancel, yield)
		}
	}
}

// runWorker consumes workChan until it is closed or poolCtx is done.
// Panics from worker are recovered and forwarded as a result error.
func runWorker[In, Out any](
	ctx context.Context,
	worker func(context.Context, In) (Out, error),
	workChan <-chan In,
	resultChan chan<- workerResult[Out],
) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, open := <-workChan:
			if !open {
				return
			}
			res, err := safeInvokeWorker(ctx, worker, item)
			select {
			case <-ctx.Done():
				return
			case resultChan <- workerResult[Out]{item: res, err: err}:
			}
		}
	}
}

// safeInvokeWorker wraps the worker call with a recover so a panicking
// handler becomes a per-item error instead of a process crash.
func safeInvokeWorker[In, Out any](
	ctx context.Context,
	worker func(context.Context, In) (Out, error),
	item In,
) (out Out, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero Out
			out = zero
			err = fmt.Errorf("stream.WorkerPool: worker panicked: %v", r)
		}
	}()
	return worker(ctx, item)
}

// feedUpstream iterates the upstream iterator and pushes items onto
// workChan. It closes workChan on exit and reports the terminal error
// (or nil) on the returned channel.
//
// A panic inside the upstream iterator is recovered and reported as a
// feed error, matching the worker-level isolation.
func feedUpstream[In any](
	ctx context.Context,
	upstream iter.Seq2[In, error],
	workChan chan<- In,
) (feedErr error) {
	defer close(workChan)
	defer func() {
		if r := recover(); r != nil {
			feedErr = fmt.Errorf("stream.WorkerPool: upstream panicked: %v", r)
		}
	}()

	for item, err := range upstream {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case workChan <- item:
		}
	}
	return nil
}

// drainResults forwards worker results to the consumer.
//
// feedDone is a buffered channel written exactly once by the feeder and
// never closed. Reading from it means the feeder has finished; setting
// the local reference to nil thereafter disables the case so the select
// does not busy-spin.
//
// The non-blocking peek on feedDone when resultChan closes is required:
// when ctx is canceled while the feeder is blocked inside its iterator,
// the feeder never sends and a blocking read would deadlock.
//
// ctx.Err() gates all upstream-error reporting: once the caller's
// context is canceled, any upstream error is post-cancellation noise
// and is dropped rather than surfaced alongside the cancellation.
func drainResults[Out any](
	ctx context.Context,
	resultChan <-chan workerResult[Out],
	feedDone <-chan error,
	cancel context.CancelFunc,
	yield func(Out, error) bool,
) {
	var upstreamError error

	for {
		select {
		case err := <-feedDone:
			if err != nil && ctx.Err() == nil {
				upstreamError = err
			}
			feedDone = nil

		case res, ok := <-resultChan:
			if !ok {
				if feedDone != nil {
					select {
					case err := <-feedDone:
						if err != nil && ctx.Err() == nil {
							upstreamError = err
						}
					default:
					}
				}
				if upstreamError != nil {
					var zero Out
					yield(zero, upstreamError)
				}
				return
			}
			if !yield(res.item, res.err) {
				cancel()
				return
			}
		}
	}
}
