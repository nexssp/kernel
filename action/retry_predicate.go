package action

import (
	"context"
	"time"

	"github.com/nexssp/kernel/xerr"
)

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

					timer := time.NewTimer(backoff(attempt + 1))
					select {
					case <-ctx.Done():
						timer.Stop()
						return res, ctx.Err()
					case <-timer.C:
					}
				}
			}

			return res, err
		}
	}
}
