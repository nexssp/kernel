// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultAsyncMaxConcurrency bounds FanOut, Race, and parallel composition
	// when the caller does not choose a positive limit.
	DefaultAsyncMaxConcurrency = 16
	// DefaultAsyncTimeout sets the default overall context deadline for FanOut,
	// Race, and parallel composition. Callbacks must observe context to stop.
	DefaultAsyncTimeout = 2 * time.Minute
)

// AsyncOptions controls the concurrency and overall duration of a batch action.
// Non-positive fields use their conservative defaults. A timeout signals
// cancellation through context; it cannot stop a callback that ignores context.
type AsyncOptions struct {
	MaxConcurrency int
	Timeout        time.Duration
}

func (o AsyncOptions) withDefaults() AsyncOptions {
	if o.MaxConcurrency <= 0 {
		o.MaxConcurrency = DefaultAsyncMaxConcurrency
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultAsyncTimeout
	}
	return o
}

// AsyncResult carries the outcome of an asynchronous action execution.
type AsyncResult[Res any] struct {
	Value Res
	Err   error
}

// Async executes the action in a goroutine and returns a result channel.
func Async[Req, Res any](ctx context.Context, act *BuiltAction[Req, Res], req Req) <-chan AsyncResult[Res] {
	ch := make(chan AsyncResult[Res], 1)
	go func() {
		res, err := act.Do(ctx, req)
		ch <- AsyncResult[Res]{Value: res, Err: err}
		close(ch)
	}()
	return ch
}

// FanOut executes the action for each request, preserving input order. A
// positive maxConcurrency is honored; a non-positive value uses
// DefaultAsyncMaxConcurrency. The overall call uses DefaultAsyncTimeout unless
// the parent context has an earlier deadline.
func FanOut[Req, Res any](
	ctx context.Context,
	act *BuiltAction[Req, Res],
	reqs []Req,
	maxConcurrency int,
) []AsyncResult[Res] {
	return FanOutWithOptions(ctx, act, reqs, AsyncOptions{MaxConcurrency: maxConcurrency})
}

// FanOutWithOptions executes the action for each request, preserving input
// order. Zero or negative option fields select their defaults. Cancellation
// and timeout are passed to callbacks through context; started callbacks are
// joined before return, so callbacks that ignore context can delay completion.
func FanOutWithOptions[Req, Res any](
	ctx context.Context,
	act *BuiltAction[Req, Res],
	reqs []Req,
	options AsyncOptions,
) []AsyncResult[Res] {
	if len(reqs) == 0 {
		return nil
	}
	return runAsyncTasks(ctx, len(reqs), options, func(ctx context.Context, index int) (Res, error) {
		return act.Do(ctx, reqs[index])
	})
}

// Race executes up to DefaultAsyncMaxConcurrency candidates at a time, with
// DefaultAsyncTimeout, and returns the first successful result. It cancels and
// joins all started candidates before returning. Use RaceWithOptions to choose a
// different limit or timeout. If all candidates fail, the last error by
// completion order is returned.
func Race[Req, Res any](
	ctx context.Context,
	act *BuiltAction[Req, Res],
	reqs []Req,
) (Res, error) {
	return RaceWithOptions(ctx, act, reqs, AsyncOptions{})
}

// RaceWithOptions executes up to the configured number of candidates at a time
// and returns the first successful result. Success, cancellation, and timeout
// cancel and join all started candidates before returning. The timeout is a
// cooperative cancellation signal, not a hard return deadline; a callback that
// ignores context can delay completion.
func RaceWithOptions[Req, Res any](
	ctx context.Context,
	act *BuiltAction[Req, Res],
	reqs []Req,
	options AsyncOptions,
) (Res, error) {
	if len(reqs) == 0 {
		var zero Res
		return zero, nil
	}

	options = options.withDefaults()
	limit := min(options.MaxConcurrency, len(reqs))
	runCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	type outcome struct {
		res Res
		err error
	}
	ch := make(chan outcome, limit)
	launch := func(req Req) {
		go func() {
			res, err := act.Do(runCtx, req)
			ch <- outcome{res: res, err: err}
		}()
	}

	next, active := 0, 0
	var lastErr error
	var winner Res
	var won bool
	var operationErr error
	done := runCtx.Done()
	for next < len(reqs) || active > 0 {
		for !won && operationErr == nil && next < len(reqs) && active < limit && runCtx.Err() == nil {
			launch(reqs[next])
			next++
			active++
		}
		if active == 0 {
			break
		}
		select {
		case result := <-ch:
			active--
			if !won && operationErr == nil && runCtx.Err() == nil && result.err == nil {
				winner = result.res
				won = true
				cancel()
				done = nil
			} else if result.err != nil {
				lastErr = result.err
			}
		case <-done:
			operationErr = runCtx.Err()
			cancel()
			done = nil
		}
	}
	if won {
		return winner, nil
	}
	if operationErr == nil {
		operationErr = runCtx.Err()
	}
	if operationErr != nil {
		var zero Res
		return zero, operationErr
	}
	var zero Res
	return zero, lastErr
}

func runAsyncTasks[Res any](
	ctx context.Context,
	count int,
	options AsyncOptions,
	run func(context.Context, int) (Res, error),
) []AsyncResult[Res] {
	if count == 0 {
		return nil
	}
	options = options.withDefaults()
	limit := min(options.MaxConcurrency, count)
	runCtx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	results := make([]AsyncResult[Res], count)
	tasks := make(chan int, limit)

	var workers sync.WaitGroup
	workers.Add(limit)
	for range limit {
		go func() {
			defer workers.Done()
			for index := range tasks {
				if err := runCtx.Err(); err != nil {
					results[index].Err = err
					continue
				}
				value, err := run(runCtx, index)
				results[index] = AsyncResult[Res]{Value: value, Err: err}
			}
		}()
	}

enqueue:
	for index := range count {
		select {
		case tasks <- index:
		case <-runCtx.Done():
			for remaining := index; remaining < count; remaining++ {
				results[remaining].Err = runCtx.Err()
			}
			break enqueue
		}
	}
	close(tasks)
	workers.Wait()
	return results
}
