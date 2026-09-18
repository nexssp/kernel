package xctx

import "context"

// ── Setters ──────────────────────────────────────────────────────────────────
//
// Each With* returns ctx with the field set on the request's scope. If no
// scope exists yet, one is created. Setters that accept an id are no-ops
// for empty strings, so callers can safely thread optional values through.

func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	ctx, s := ensureScope(ctx)
	s.RequestID = id
	return ctx
}

func WithExecutionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	ctx, s := ensureScope(ctx)
	s.ExecutionID = id
	return ctx
}

func WithRootExecutionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	ctx, s := ensureScope(ctx)
	s.RootExecutionID = id
	return ctx
}

func WithTraceID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	ctx, s := ensureScope(ctx)
	s.TraceID = id
	return ctx
}

func WithSpanID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	ctx, s := ensureScope(ctx)
	s.SpanID = id
	return ctx
}

// WithTraceContext sets TraceID and/or SpanID in one call. Empty strings
// are ignored, so passing ("trace", "") sets only the trace ID.
func WithTraceContext(ctx context.Context, traceID, spanID string) context.Context {
	if traceID == "" && spanID == "" {
		return ctx
	}
	ctx, s := ensureScope(ctx)
	if traceID != "" {
		s.TraceID = traceID
	}
	if spanID != "" {
		s.SpanID = spanID
	}
	return ctx
}

func WithEndpoint(ctx context.Context, endpoint string) context.Context {
	ctx, s := ensureScope(ctx)
	s.Endpoint = endpoint
	return ctx
}

func WithTenantID(ctx context.Context, id string) context.Context {
	ctx, s := ensureScope(ctx)
	s.TenantID = id
	return ctx
}

func WithUserID(ctx context.Context, id string) context.Context {
	ctx, s := ensureScope(ctx)
	s.UserID = id
	return ctx
}

func WithClientIP(ctx context.Context, ip string) context.Context {
	ctx, s := ensureScope(ctx)
	s.ClientIP = ip
	return ctx
}

// WithRoles replaces the roles slice and sets Role to the first element.
// Passing nil clears both.
func WithRoles(ctx context.Context, roles []string) context.Context {
	ctx, s := ensureScope(ctx)
	s.Roles = append(s.Roles[:0], roles...)
	s.Role = ""
	if len(roles) > 0 {
		s.Role = roles[0]
	}
	return ctx
}

func WithFeatures(ctx context.Context, features []string) context.Context {
	ctx, s := ensureScope(ctx)
	s.Features = append(s.Features[:0], features...)
	return ctx
}

func WithPermissions(ctx context.Context, perms []string) context.Context {
	ctx, s := ensureScope(ctx)
	s.Permissions = append(s.Permissions[:0], perms...)
	return ctx
}

// ── Getters ──────────────────────────────────────────────────────────────────
//
// Each *From returns the field or the zero value if the scope is missing.

func RequestIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.RequestID
	}
	return ""
}

func ExecutionIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.ExecutionID
	}
	return ""
}

// RootExecutionIDFrom returns the top-level execution ID recorded by
// WithRootExecutionID, or "" when none was set.
func RootExecutionIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.RootExecutionID
	}
	return ""
}

func TenantIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.TenantID
	}
	return ""
}

func UserIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.UserID
	}
	return ""
}

func EndpointFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.Endpoint
	}
	return ""
}

func TraceIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.TraceID
	}
	return ""
}

func SpanIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.SpanID
	}
	return ""
}

func ClientIPFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil {
		return s.ClientIP
	}
	return ""
}

func FeaturesFrom(ctx context.Context) []string {
	if s := ScopeFrom(ctx); s != nil {
		return s.Features
	}
	return nil
}

func PermissionsFrom(ctx context.Context) []string {
	if s := ScopeFrom(ctx); s != nil {
		return s.Permissions
	}
	return nil
}

// ── Bulk population ──────────────────────────────────────────────────────────

// FromClaims populates a scope from a decoded JWT claim map. Recognized
// keys: sub, ten, jti, roles, features, perms. Unknown keys are ignored.
func FromClaims(scope *RequestScope, claims map[string]any) {
	if scope == nil || len(claims) == 0 {
		return
	}
	if v, _ := claims["sub"].(string); v != "" {
		scope.UserID = v
	}
	if v, _ := claims["ten"].(string); v != "" {
		scope.TenantID = v
	}
	if scope.RequestID == "" {
		if v, _ := claims["jti"].(string); v != "" {
			scope.RequestID = v
		}
	}
	if roles := toStringSlice(claims["roles"]); len(roles) > 0 {
		scope.Roles = append(scope.Roles[:0], roles...)
		scope.Role = scope.Roles[0]
	}
	if feats := toStringSlice(claims["features"]); len(feats) > 0 {
		scope.Features = append(scope.Features[:0], feats...)
	}
	if perms := toStringSlice(claims["perms"]); len(perms) > 0 {
		scope.Permissions = append(scope.Permissions[:0], perms...)
	}
}

func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		s := make([]string, 0, len(t))
		for _, e := range t {
			if str, ok := e.(string); ok {
				s = append(s, str)
			}
		}
		return s
	}
	return nil
}
