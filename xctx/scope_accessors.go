package xctx

import (
	"context"
	"slices"
)

var (
	executionIDKey     = NewKey[string]("xctx.execution_id")
	rootExecutionIDKey = NewKey[string]("xctx.root_execution_id")
)

// ── Setters ──────────────────────────────────────────────────────────────────
//
// Each With* returns ctx with the field set. Setters that accept an id are
// no-ops for empty strings, so callers can safely thread optional values
// through. Execution identity setters write to context keys; the rest
// write to the RequestScope.

func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return withLockedScope(ctx, func(scope *RequestScope) { scope.RequestID = id })
}

// WithExecutionID stores the id in context keys, not in a RequestScope.
// The root id is set only when no root exists yet in the context tree.
func WithExecutionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	ctx = executionIDKey.With(ctx, id)
	if RootExecutionIDFrom(ctx) == "" {
		ctx = rootExecutionIDKey.With(ctx, id)
	}
	return ctx
}

func WithRootExecutionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return rootExecutionIDKey.With(ctx, id)
}

func WithTraceID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return withLockedScope(ctx, func(scope *RequestScope) { scope.TraceID = id })
}

func WithSpanID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return withLockedScope(ctx, func(scope *RequestScope) { scope.SpanID = id })
}

// WithTraceContext sets TraceID and/or SpanID in one call. Empty strings
// are ignored, so passing ("trace", "") sets only the trace ID.
func WithTraceContext(ctx context.Context, traceID, spanID string) context.Context {
	if traceID == "" && spanID == "" {
		return ctx
	}
	return withLockedScope(ctx, func(scope *RequestScope) {
		if traceID != "" {
			scope.TraceID = traceID
		}
		if spanID != "" {
			scope.SpanID = spanID
		}
	})
}

func WithEndpoint(ctx context.Context, endpoint string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) { scope.Endpoint = endpoint })
}

func WithTenantID(ctx context.Context, id string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) { scope.TenantID = id })
}

func WithUserID(ctx context.Context, id string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) { scope.UserID = id })
}

func WithClientIP(ctx context.Context, ip string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) { scope.ClientIP = ip })
}

// WithRoles replaces the roles slice and sets Role to the first element.
// Passing nil clears both.
func WithRoles(ctx context.Context, roles []string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) {
		scope.Roles = append(scope.Roles[:0], roles...)
		scope.Role = ""
		if len(roles) > 0 {
			scope.Role = roles[0]
		}
	})
}

func WithFeatures(ctx context.Context, features []string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) {
		scope.Features = append(scope.Features[:0], features...)
	})
}

func WithPermissions(ctx context.Context, perms []string) context.Context {
	return withLockedScope(ctx, func(scope *RequestScope) {
		scope.Permissions = append(scope.Permissions[:0], perms...)
	})
}

// ── Getters ──────────────────────────────────────────────────────────────────
//
// Each *From returns the field or the zero value if the scope is missing.

func RequestIDFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.RequestID
	}
	return ""
}

// ExecutionIDFrom returns the active execution ID from context keys.
func ExecutionIDFrom(ctx context.Context) string {
	id, _ := executionIDKey.From(ctx)
	return id
}

// RootExecutionIDFrom returns the top-level execution ID recorded by
// WithRootExecutionID, or "" when none was set.
func RootExecutionIDFrom(ctx context.Context) string {
	id, _ := rootExecutionIDKey.From(ctx)
	return id
}

func TenantIDFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.TenantID
	}
	return ""
}

func UserIDFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.UserID
	}
	return ""
}

func EndpointFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.Endpoint
	}
	return ""
}

func TraceIDFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.TraceID
	}
	return ""
}

func SpanIDFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.SpanID
	}
	return ""
}

func ClientIPFrom(ctx context.Context) string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return s.ClientIP
	}
	return ""
}

// ── Bulk population ──────────────────────────────────────────────────────────

// FromClaims populates a scope from a decoded JWT claim map. Recognized
// keys: sub, ten, jti, roles, features, perms. Unknown keys are ignored.
// It acquires scope.mu to synchronize writes with xctx accessors; direct field
// access still requires caller synchronization.
func FromClaims(scope *RequestScope, claims map[string]any) {
	if scope == nil || len(claims) == 0 {
		return
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
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

// RolesFrom returns a defensive copy of the scope's roles. Mutating the
// result has no effect on the scope.
func RolesFrom(ctx context.Context) []string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return slices.Clone(s.Roles)
	}
	return nil
}

// FeaturesFrom returns a defensive copy of the scope's features.
func FeaturesFrom(ctx context.Context) []string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return slices.Clone(s.Features)
	}
	return nil
}

// PermissionsFrom returns a defensive copy of the scope's permissions.
func PermissionsFrom(ctx context.Context) []string {
	if s := lockScopeForRead(ctx); s != nil {
		defer s.mu.RUnlock()
		return slices.Clone(s.Permissions)
	}
	return nil
}

// ScopeSnapshot is a stable copy of request identity and authorization
// metadata. Its slices are owned by the snapshot; TraceEvents is excluded.
type ScopeSnapshot struct {
	Present     bool // false when no live scope was bound to the context.
	RequestID   string
	TraceID     string
	SpanID      string
	Endpoint    string
	UserID      string
	TenantID    string
	Role        string
	ClientIP    string
	Roles       []string
	Permissions []string
	Features    []string
}

// SnapshotFrom returns a stable identity/authorization copy of the live scope,
// or a zero snapshot when ctx has no live scope. It omits TraceEvents. Slice
// fields are copied so later scope changes cannot change the returned snapshot.
func SnapshotFrom(ctx context.Context) ScopeSnapshot {
	scope := lockScopeForRead(ctx)
	if scope == nil {
		return ScopeSnapshot{}
	}
	defer scope.mu.RUnlock()
	return ScopeSnapshot{
		Present:     true,
		RequestID:   scope.RequestID,
		TraceID:     scope.TraceID,
		SpanID:      scope.SpanID,
		Endpoint:    scope.Endpoint,
		UserID:      scope.UserID,
		TenantID:    scope.TenantID,
		Role:        scope.Role,
		ClientIP:    scope.ClientIP,
		Roles:       slices.Clone(scope.Roles),
		Permissions: slices.Clone(scope.Permissions),
		Features:    slices.Clone(scope.Features),
	}
}
