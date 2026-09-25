// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// RetryMiddleware is the backward-compatible, safe default retry middleware.
// It retries only transient errors detected by xerr.IsTransient.
func RetryMiddleware[Req, Res any](
	maxRetry int,
	backoff func(attempt int) time.Duration,
) DispatcherMiddleware[Req, Res] {
	return RetryWithPredicateMiddleware[Req, Res](maxRetry, backoff, DefaultRetryPredicate)
}

// RetryPredicate determines whether a given error warrants an execution retry.
type RetryPredicate func(err error) bool

// DefaultRetryPredicate retries only errors flagged as transient by xerr.
func DefaultRetryPredicate(err error) bool {
	return xerr.IsTransient(err)
}

// AlwaysRetryPredicate retries any non-nil error.
func AlwaysRetryPredicate(err error) bool {
	return err != nil
}

// timerPool reuses *time.Timer instances across retry attempts to avoid
// the 2 allocations (timer struct + channel) per attempt that time.NewTimer
// would otherwise incur.
//
// Each timer is Reset before use and Stop()'d before returning to the pool.
// Stopped timers can be Reset() safely per Go 1.23+ semantics.
var timerPool = sync.Pool{
	New: func() any {
		t := time.NewTimer(0)
		t.Stop()
		return t
	},
}

// RetryWithPredicateMiddleware retries errors when predicate returns true.
// If predicate is nil, it falls back to DefaultRetryPredicate.
// If backoff is nil, it defaults to ConstantBackoff(0).
// If maxRetry is negative, it is clamped to 0.
func RetryWithPredicateMiddleware[Req, Res any](
	maxRetry int,
	backoff func(attempt int) time.Duration,
	predicate RetryPredicate,
) DispatcherMiddleware[Req, Res] {
	if maxRetry < 0 {
		maxRetry = 0
	}
	if backoff == nil {
		backoff = ConstantBackoff(0)
	}
	if predicate == nil {
		predicate = DefaultRetryPredicate
	}

	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			for attempt := 0; attempt <= maxRetry; attempt++ {
				res, err = next(ctx, req)

				if err == nil {
					return res, nil
				}

				if !predicate(err) {
					return res, err
				}

				if attempt < maxRetry {
					if hooks != nil {
						hooks.OnRetry(ctx, req, attempt+1, err)
					}

					// Use pooled timer to avoid time.NewTimer's 2 allocations
					// (timer struct + channel) per retry attempt.
					timer, ok := timerPool.Get().(*time.Timer)
					if !ok || timer == nil {
						timer = time.NewTimer(backoff(attempt + 1))
					} else {
						timer.Reset(backoff(attempt + 1))
					}
					select {
					case <-ctx.Done():
						timer.Stop()
						select {
						case <-timer.C:
						default:
						}
						timerPool.Put(timer)
						return res, ctx.Err()
					case <-timer.C:
						timerPool.Put(timer)
					}
				}
			}

			return res, err
		}
	}
}
