package action

import "time"

// RetryMiddleware is the backward-compatible, safe default retry middleware.
// It retries only transient errors detected by xerr.IsTransient.
func RetryMiddleware[Req, Res any](
	maxRetry int,
	backoff func(attempt int) time.Duration,
) DispatcherMiddleware[Req, Res] {
	return RetryWithPredicateMiddleware[Req, Res](maxRetry, backoff, DefaultRetryPredicate)
}
