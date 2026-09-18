// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

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
