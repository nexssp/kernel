package action

import "time"

// Retry retries only transient errors detected by xerr.IsTransient.
func (b *Builder[Req, Res]) Retry(
	maxRetry int,
	backoff func(attempt int) time.Duration,
) *Builder[Req, Res] {
	b.meta.RetryMax = maxRetry
	return b.UseWithDispatcher(RetryMiddleware[Req, Res](maxRetry, backoff))
}

// RetryIf retries when the supplied predicate returns true.
func (b *Builder[Req, Res]) RetryIf(
	maxRetry int,
	backoff func(attempt int) time.Duration,
	predicate RetryPredicate,
) *Builder[Req, Res] {
	b.meta.RetryMax = maxRetry
	return b.UseWithDispatcher(
		RetryWithPredicateMiddleware[Req, Res](maxRetry, backoff, predicate),
	)
}

// RetryAll retries on ANY non-nil error.
// Use only for idempotent jobs, scripts, and safe batch operations.
func (b *Builder[Req, Res]) RetryAll(
	maxRetry int,
	backoff func(attempt int) time.Duration,
) *Builder[Req, Res] {
	b.meta.RetryMax = maxRetry
	return b.UseWithDispatcher(
		RetryWithPredicateMiddleware[Req, Res](maxRetry, backoff, AlwaysRetryPredicate),
	)
}
