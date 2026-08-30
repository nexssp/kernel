package action

import (
	"context"

	"github.com/nexssp/kernel/xerr"
)

// ImmutableWhen aborts execution with 403 Forbidden if the guard condition returns true.
// Useful for locking entities in terminal or processed states (e.g. settled invoices).
func (b *Builder[Req, Res]) ImmutableWhen(guard func(ctx context.Context, req Req) (bool, error), reason string) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if guard != nil {
				locked, err := guard(ctx, req)
				if err != nil {
					var zero Res
					return zero, err
				}
				if locked {
					var zero Res
					if reason == "" {
						reason = "modification forbidden: resource is locked in current state"
					}
					return zero, xerr.Forbidden(reason)
				}
			}
			return next(ctx, req)
		}
	})
}
