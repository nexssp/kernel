package action

import (
	"context"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// SmartResilience automatically applies intelligent backoff and circuit breaking
// based entirely on the xerr.Kind of the returned error.
// It embodies the "Inferred Resilience" pattern.
func SmartResilience[Req, Res any](name string) Middleware[Req, Res] {
	// Internal adaptive circuit breaker shared across all calls to this action
	cb := Adaptive[Req, Res](name, AdaptiveConfig{
		FailureThreshold: 5,
		ResetTimeout:     10 * time.Second,
		InitialTimeout:   3 * time.Second,
	})

	backoff := ExponentialJitter(100*time.Millisecond, 2*time.Second)

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		// Wrap with Circuit Breaker
		protectedNext := cb(next)

		return func(ctx context.Context, req Req) (Res, error) {
			const maxAttempts = 3
			var res Res
			var err error

			for attempt := range maxAttempts {
				res, err = protectedNext(ctx, req)
				if err == nil {
					return res, nil
				}

				// Look into the taxonomy of the error
				appErr := xerr.From(err)

				// Fast-Fail logic: Do NOT retry user errors (4xx)
				switch appErr.Kind {
				case xerr.KindBadRequest, xerr.KindUnauthorized, xerr.KindForbidden, xerr.KindNotFound, xerr.KindValidation:
					return res, err
				}

				// If it's the last attempt, don't sleep
				if attempt == maxAttempts-1 {
					break
				}

				// Wait according to backoff
				timer := time.NewTimer(backoff(attempt + 1))
				select {
				case <-ctx.Done():
					timer.Stop()
					return res, ctx.Err()
				case <-timer.C:
				}
			}

			return res, err
		}
	}
}

// InferredResilient applies the smart defaults to a Builder
func (b *Builder[Req, Res]) InferredResilient() *Builder[Req, Res] {
	return b.Use(SmartResilience[Req, Res](b.meta.Name))
}
