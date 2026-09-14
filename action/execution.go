package action

import (
	"context"

	"github.com/nexssp/kernel/xctx"
)

func WithExecutionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return xctx.WithExecutionID(ctx, id)
}

func ExecutionIDFrom(ctx context.Context) string {
	return xctx.ExecutionIDFrom(ctx)
}
