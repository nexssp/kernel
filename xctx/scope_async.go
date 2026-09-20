package xctx

import "context"

// CloneForAsync returns a context whose scope is a deep copy of ctx's,
// detached from ctx's cancellation. Use before handing ctx to a
// goroutine that outlives the request (background publish, batch flush,
// best-effort audit) so that canceling the parent does not abort the
// detached work and the detached work does not race the parent's pooled
// scope.
func CloneForAsync(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	s := ScopeFrom(ctx)
	if s == nil {
		return context.WithoutCancel(ctx)
	}

	// Snapshot TraceEvents under the lock; the parent request may still
	// have in-flight goroutines calling AddTrace.
	s.traceMu.Lock()
	traceEvents := append([]string(nil), s.TraceEvents...)
	s.traceMu.Unlock()

	// Seed a fresh generation so AddTrace writes to the clone, not the
	// (possibly recycled) parent scope.
	gen := globalGeneration.Add(1)

	clone := &RequestScope{
		generation:      gen,
		RequestID:       s.RequestID,
		ExecutionID:     s.ExecutionID,
		RootExecutionID: s.RootExecutionID,
		TraceID:         s.TraceID,
		SpanID:          s.SpanID,
		Endpoint:        s.Endpoint,
		UserID:          s.UserID,
		TenantID:        s.TenantID,
		Role:            s.Role,
		ClientIP:        s.ClientIP,
		TraceEvents:     traceEvents,
	}

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
	return context.WithValue(asyncCtx, scopeGenKeyT{}, gen)
}
