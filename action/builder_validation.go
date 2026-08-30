package action

import (
	"context"
	"errors"

	"github.com/nexssp/kernel/xerr"
)

// Validate adds a request validation middleware to the action construction pipeline.
func (b *Builder[Req, Res]) Validate(fn func(ctx context.Context, req Req) error) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if fn != nil {
				if err := fn(ctx, req); err != nil {
					var zero Res
					var appErr *xerr.AppError
					if errors.As(err, &appErr) {
						return zero, appErr
					}
					return zero, xerr.Validation("validation failed", err)
				}
			}
			return next(ctx, req)
		}
	})
}
