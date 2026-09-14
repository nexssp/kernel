package xctx

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

const maxTraceEvents = 128

type scopeKeyT struct{}

type Key[T any] struct {
	name string
}

func NewKey[T any](name string) Key[T] {
	return Key[T]{name: name}
}

func (k Key[T]) With(ctx context.Context, val T) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, k, val)
}

func (k Key[T]) From(ctx context.Context) (T, bool) {
	if ctx == nil {
		var zero T
		return zero, false
	}
	v, ok := ctx.Value(k).(T)
	return v, ok
}

func (k Key[T]) MustFrom(ctx context.Context) T {
	v, ok := k.From(ctx)
	if !ok {
		panic(fmt.Sprintf("xctx: key %q not found in context", k.name))
	}
	return v
}

type RequestScope struct {
	RequestID   string
	ExecutionID string
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
	TraceEvents []string
}

func (s *RequestScope) Reset() {
	s.RequestID = ""
	s.ExecutionID = ""
	s.TraceID = ""
	s.SpanID = ""
	s.Endpoint = ""
	s.UserID = ""
	s.TenantID = ""
	s.Role = ""
	s.ClientIP = ""
	s.Roles = s.Roles[:0]
	s.Permissions = s.Permissions[:0]
	s.Features = s.Features[:0]
	if cap(s.TraceEvents) > maxTraceEvents {
		s.TraceEvents = nil
	} else {
		s.TraceEvents = s.TraceEvents[:0]
	}
}

func AddTrace(ctx context.Context, event string) {
	s := ScopeFrom(ctx)
	if s == nil {
		return
	}
	if len(s.TraceEvents) >= maxTraceEvents {
		copy(s.TraceEvents, s.TraceEvents[1:])
		s.TraceEvents = s.TraceEvents[:maxTraceEvents-1]
	}
	s.TraceEvents = append(s.TraceEvents, event)
}

var scopePool = sync.Pool{New: func() any { return &RequestScope{} }}

func NewScope(parent context.Context) (context.Context, *RequestScope, func()) {
	if parent == nil {
		parent = context.Background()
	}
	s := scopePool.Get().(*RequestScope)
	ctx := context.WithValue(parent, scopeKeyT{}, s)
	var once sync.Once
	return ctx, s, func() {
		once.Do(func() {
			s.Reset()
			scopePool.Put(s)
		})
	}
}

func ScopeFrom(ctx context.Context) *RequestScope {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(scopeKeyT{}).(*RequestScope)
	return s
}

func WithScope(ctx context.Context, s *RequestScope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, scopeKeyT{}, s)
}

func ensureScope(ctx context.Context) (context.Context, *RequestScope) {
	if ctx == nil {
		ctx = context.Background()
	}
	s, _ := ctx.Value(scopeKeyT{}).(*RequestScope)
	if s != nil {
		return ctx, s
	}
	s = &RequestScope{}
	return context.WithValue(ctx, scopeKeyT{}, s), s
}

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

func WithRequestID(ctx context.Context, id string) context.Context {
	ctx, s := ensureScope(ctx)
	s.RequestID = id
	return ctx
}

func WithExecutionID(ctx context.Context, id string) context.Context {
	ctx, s := ensureScope(ctx)
	s.ExecutionID = id
	return ctx
}

func WithTraceID(ctx context.Context, id string) context.Context {
	ctx, s := ensureScope(ctx)
	s.TraceID = id
	return ctx
}

func WithSpanID(ctx context.Context, id string) context.Context {
	ctx, s := ensureScope(ctx)
	s.SpanID = id
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

func WithClientIP(ctx context.Context, ip string) context.Context {
	ctx, s := ensureScope(ctx)
	s.ClientIP = ip
	return ctx
}

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

func HasRole(ctx context.Context, required string) bool {
	if s := ScopeFrom(ctx); s != nil {
		if s.Role == required {
			return true
		}
		return slices.Contains(s.Roles, required)
	}
	return false
}

func HasAnyRole(ctx context.Context, allowed ...string) bool {
	if s := ScopeFrom(ctx); s != nil {
		for _, r := range allowed {
			if s.Role == r || slices.Contains(s.Roles, r) {
				return true
			}
		}
	}
	return false
}

func HasPermission(ctx context.Context, perm string) bool {
	if s := ScopeFrom(ctx); s != nil {
		for _, p := range s.Permissions {
			if p == perm || p == "*" {
				return true
			}
		}
	}
	return false
}

func HasFeature(ctx context.Context, feature string) bool {
	if HasRole(ctx, "system_admin") {
		return true
	}
	if s := ScopeFrom(ctx); s != nil {
		for _, f := range s.Features {
			if f == feature || f == "all" || f == "*" {
				return true
			}
		}
	}
	return false
}

func CloneForAsync(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	s := ScopeFrom(ctx)
	if s == nil {
		return context.WithoutCancel(ctx)
	}

	clone := &RequestScope{
		RequestID:   s.RequestID,
		ExecutionID: s.ExecutionID,
		TraceID:     s.TraceID,
		SpanID:      s.SpanID,
		Endpoint:    s.Endpoint,
		UserID:      s.UserID,
		TenantID:    s.TenantID,
		Role:        s.Role,
		ClientIP:    s.ClientIP,
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
	if s.TraceEvents != nil {
		clone.TraceEvents = append([]string(nil), s.TraceEvents...)
	}

	return context.WithValue(context.WithoutCancel(ctx), scopeKeyT{}, clone)
}
