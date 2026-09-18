// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

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
