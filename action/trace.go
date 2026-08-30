package action

import "context"

type traceContextKey struct{}

type traceContext struct {
	traceID string
	spanID  string
}

// WithTraceContext attaches optional distributed-trace identifiers to ctx.
// The transport or tracing adapter owns ID generation and propagation.
func WithTraceContext(ctx context.Context, traceID, spanID string) context.Context {
	if traceID == "" && spanID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey{}, traceContext{traceID: traceID, spanID: spanID})
}

// TraceIDFrom returns the optional distributed trace ID from ctx.
func TraceIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(traceContextKey{}).(traceContext)
	return value.traceID
}

// SpanIDFrom returns the optional current span ID from ctx.
func SpanIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(traceContextKey{}).(traceContext)
	return value.spanID
}
