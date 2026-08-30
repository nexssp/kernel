package action

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrConcurrencyLimit = errors.New("concurrency limit exceeded")

func ConcurrencyLimitMiddleware[Req, Res any](limit int32) Middleware[Req, Res] {
	var count atomic.Int32

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if count.Add(1) > limit {
				count.Add(-1)
				var zero Res
				return zero, ErrConcurrencyLimit
			}
			defer count.Add(-1)

			return next(ctx, req)
		}
	}
}
