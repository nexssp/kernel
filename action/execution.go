package action

import "context"

type executionIDKey struct{}

// WithExecutionID attaches an optional correlation ID to an action context.
// Callers should provide a request, trace, or action-execution ID at the
// transport or application boundary. Empty IDs are treated as absent.
func WithExecutionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, executionIDKey{}, id)
}

// ExecutionIDFrom returns the optional correlation ID attached to ctx.
func ExecutionIDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(executionIDKey{}).(string)
	return id
}
