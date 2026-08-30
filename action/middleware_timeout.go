package action

import (
	"context"
	"time"
)

// TimeoutMiddleware uses standard Middleware.
func TimeoutMiddleware[Req, Res any](d time.Duration) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, req)
		}
	}
}
