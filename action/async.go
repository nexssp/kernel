package action

import (
	"context"
)

// ── Async execution ─────────────────────────────────────────────────────────────

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

// ── FanOut: Parallel execution with concurrency limit ──────────────────────────

// FanOut executes the action concurrently for each request.
// maxConcurrency limits how many run simultaneously; 0 = unbounded.
func FanOut[Req, Res any](
	ctx context.Context,
	act *BuiltAction[Req, Res],
	reqs []Req,
	maxConcurrency int,
) []AsyncResult[Res] {
	if len(reqs) == 0 {
		return nil
	}
	if maxConcurrency <= 0 || maxConcurrency > len(reqs) {
		maxConcurrency = len(reqs)
	}

	type result struct {
		idx int
		val AsyncResult[Res]
	}

	results := make([]AsyncResult[Res], len(reqs))
	sem := make(chan struct{}, maxConcurrency)
	resCh := make(chan result, len(reqs))

	var launched int
	for i, req := range reqs {
		select {
		case <-ctx.Done():
			results[i] = AsyncResult[Res]{Err: ctx.Err()}
			continue
		case sem <- struct{}{}:
		}

		launched++
		go func(idx int, r Req) {
			defer func() { <-sem }()
			res, err := act.Do(ctx, r)
			resCh <- result{idx, AsyncResult[Res]{Value: res, Err: err}}
		}(i, req)
	}

	for range launched {
		r := <-resCh
		results[r.idx] = r.val
	}
	return results
}

// ── Race: First success wins ──────────────────────────────────────────────────

// Race executes the action concurrently for each request and returns the first success.
func Race[Req, Res any](
	ctx context.Context,
	act *BuiltAction[Req, Res],
	reqs []Req,
) (Res, error) {
	if len(reqs) == 0 {
		var zero Res
		return zero, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type outcome struct {
		res Res
		err error
	}
	ch := make(chan outcome, len(reqs))

	for _, req := range reqs {
		go func(r Req) {
			res, err := act.Do(ctx, r)
			ch <- outcome{res, err}
		}(req)
	}

	var lastErr error
	for range reqs {
		o := <-ch
		if o.err == nil {
			cancel()
			return o.res, nil
		}
		lastErr = o.err
	}

	var zero Res
	return zero, lastErr
}
