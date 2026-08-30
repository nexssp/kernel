package action

import (
	"context"
	"time"

	"github.com/nexssp/kernel/xerr"
)

type TransientChecker interface {
	IsTransient() bool
}

// RetryMiddleware returns a DispatcherMiddleware (Fixing the compile error).
func RetryMiddleware[Req, Res any](maxRetry int, backoff func(attempt int) time.Duration) DispatcherMiddleware[Req, Res] {
	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			for attempt := 0; attempt <= maxRetry; attempt++ {
				res, err = next(ctx, req)

				if err == nil {
					return res, nil
				}

				if !xerr.IsTransient(err) {
					// If it's a fatal/permanent error, DO NOT retry. Return immediately.
					return res, err
				}

				if attempt < maxRetry {
					hooks.OnRetry(ctx, req, attempt+1, err)

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
