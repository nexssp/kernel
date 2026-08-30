package action

import (
	"context"
	"fmt"
)

type TxRunner interface {
	RunInTx(ctx context.Context, fn func(txCtx context.Context) error) error
}

func (b *Builder[Req, Res]) Transactional(runner TxRunner) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if runner == nil {
				return next(ctx, req)
			}

			var res Res
			err := runner.RunInTx(ctx, func(txCtx context.Context) error {
				var innerErr error
				res, innerErr = next(txCtx, req)
				if innerErr != nil {
					return fmt.Errorf("action execution failed in transaction: %w", innerErr)
				}
				return nil
			})

			return res, err
		}
	})
}
