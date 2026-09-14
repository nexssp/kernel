package action

import (
	"context"

	"github.com/nexssp/kernel/xctx"
)

func WithTraceContext(ctx context.Context, traceID, spanID string) context.Context {
	if traceID == "" && spanID == "" {
		return ctx
	}
	if traceID != "" {
		ctx = xctx.WithTraceID(ctx, traceID)
	}
	if spanID != "" {
		ctx = xctx.WithSpanID(ctx, spanID)
	}
	return ctx
}

func TraceIDFrom(ctx context.Context) string {
	return xctx.TraceIDFrom(ctx)
}

func SpanIDFrom(ctx context.Context) string {
	return xctx.SpanIDFrom(ctx)
}
