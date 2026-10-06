package xctx

import "context"

// CloneForAsync returns a context whose scope is a deep copy of ctx's,
// detached from ctx's cancellation. Use before handing ctx to a
// goroutine that outlives the request (background publish, batch flush,
// best-effort audit) so that canceling the parent does not abort the
// detached work and the detached work does not race the parent's pooled
// scope.
//
// Execution identity keys (execution id, root execution id) are carried
// through automatically: context.WithoutCancel preserves all values, so
// the clone sees the same execution ids as the parent without an
// explicit copy. Only the mutable RequestScope is duplicated.
func CloneForAsync(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	s := ScopeFrom(ctx)
	if s == nil {
		return context.WithoutCancel(ctx)
	}

	s.traceMu.Lock()
	traceEvents := append([]string(nil), s.TraceEvents...)
	s.traceMu.Unlock()

	gen := globalGeneration.Add(1)

	clone := &RequestScope{
		RequestID:   s.RequestID,
		TraceID:     s.TraceID,
		SpanID:      s.SpanID,
		Endpoint:    s.Endpoint,
		UserID:      s.UserID,
		TenantID:    s.TenantID,
		Role:        s.Role,
		ClientIP:    s.ClientIP,
		TraceEvents: traceEvents,
	}
	clone.generation.Store(gen)

	if s.Roles != nil {
		clone.Roles = append([]string(nil), s.Roles...)
	}
	if s.Permissions != nil {
		clone.Permissions = append([]string(nil), s.Permissions...)
	}
	if s.Features != nil {
		clone.Features = append([]string(nil), s.Features...)
	}

	asyncCtx := context.WithValue(context.WithoutCancel(ctx), scopeKeyT{}, clone)
	asyncCtx = context.WithValue(asyncCtx, scopeGenKeyT{}, gen)
	return asyncCtx
}
