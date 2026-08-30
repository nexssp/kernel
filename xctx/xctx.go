package xctx

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

type (
	traceIDKeyT     struct{}
	requestIDKeyT   struct{}
	endpointKeyT    struct{}
	tenantIDKeyT    struct{}
	userIDKeyT      struct{}
	roleKeyT        struct{}
	permissionsKeyT struct{}
	featuresKeyT    struct{}
	clientIPKeyT    struct{}
	scopeKeyT       struct{}
)

// Key is a typed context key.
type Key[T any] struct {
	name string
}

func NewKey[T any](name string) Key[T] {
	return Key[T]{name: name}
}

func (k Key[T]) With(ctx context.Context, val T) context.Context {
	return context.WithValue(ctx, k, val)
}

func (k Key[T]) From(ctx context.Context) (T, bool) {
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

// RequestScope holds all cross-cutting data for a single request.
type RequestScope struct {
	RequestID   string
	Endpoint    string
	UserID      string
	TenantID    string
	TraceID     string
	Role        string
	Roles       []string
	Permissions []string
	Features    []string
	ClientIP    string
	TraceEvents []string
}

func (s *RequestScope) Reset() {
	s.RequestID = ""
	s.Endpoint = ""
	s.UserID = ""
	s.TenantID = ""
	s.TraceID = ""
	s.Role = ""
	s.ClientIP = ""
	s.Roles = s.Roles[:0]
	s.Permissions = s.Permissions[:0]
	s.Features = s.Features[:0]
	if cap(s.TraceEvents) > 128 {
		s.TraceEvents = nil
	} else {
		s.TraceEvents = s.TraceEvents[:0]
	}
}

func AddTrace(ctx context.Context, event string) {
	if s := ScopeFrom(ctx); s != nil {
		s.TraceEvents = append(s.TraceEvents, event)
	}
}

var scopePool = sync.Pool{New: func() any { return &RequestScope{} }}

func NewScope(parent context.Context) (context.Context, *RequestScope, func()) {
	s := scopePool.Get().(*RequestScope)
	ctx := context.WithValue(parent, scopeKeyT{}, s)
	return ctx, s, func() {
		s.Reset()
		scopePool.Put(s)
	}
}

func ScopeFrom(ctx context.Context) *RequestScope {
	s, _ := ctx.Value(scopeKeyT{}).(*RequestScope)
	return s
}

func WithScope(ctx context.Context, s *RequestScope) context.Context {
	return context.WithValue(ctx, scopeKeyT{}, s)
}

func FromClaims(scope *RequestScope, claims map[string]any) {
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
	if s := ScopeFrom(ctx); s != nil {
		s.RequestID = id
	}
	return context.WithValue(ctx, requestIDKeyT{}, id)
}

func WithEndpoint(ctx context.Context, endpoint string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.Endpoint = endpoint
	}
	return context.WithValue(ctx, endpointKeyT{}, endpoint)
}

func WithTenantID(ctx context.Context, id string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.TenantID = id
	}
	return context.WithValue(ctx, tenantIDKeyT{}, id)
}

func WithUserID(ctx context.Context, id string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.UserID = id
	}
	return context.WithValue(ctx, userIDKeyT{}, id)
}

func WithRoles(ctx context.Context, roles []string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.Roles = roles
		if len(roles) > 0 {
			s.Role = roles[0]
		}
	}
	return context.WithValue(ctx, roleKeyT{}, roles)
}

func WithFeatures(ctx context.Context, features []string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.Features = features
	}
	return context.WithValue(ctx, featuresKeyT{}, features)
}

func WithPermissions(ctx context.Context, perms []string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.Permissions = perms
	}
	return context.WithValue(ctx, permissionsKeyT{}, perms)
}

func WithTraceID(ctx context.Context, id string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.TraceID = id
	}
	return context.WithValue(ctx, traceIDKeyT{}, id)
}

func WithClientIP(ctx context.Context, ip string) context.Context {
	if s := ScopeFrom(ctx); s != nil {
		s.ClientIP = ip
	}
	return context.WithValue(ctx, clientIPKeyT{}, ip)
}

func RequestIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil && s.RequestID != "" {
		return s.RequestID
	}
	id, _ := ctx.Value(requestIDKeyT{}).(string)
	return id
}

func TenantIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil && s.TenantID != "" {
		return s.TenantID
	}
	id, _ := ctx.Value(tenantIDKeyT{}).(string)
	return id
}

func UserIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil && s.UserID != "" {
		return s.UserID
	}
	id, _ := ctx.Value(userIDKeyT{}).(string)
	return id
}

func EndpointFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil && s.Endpoint != "" {
		return s.Endpoint
	}
	ep, _ := ctx.Value(endpointKeyT{}).(string)
	return ep
}

func TraceIDFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil && s.TraceID != "" {
		return s.TraceID
	}
	id, _ := ctx.Value(traceIDKeyT{}).(string)
	return id
}

func ClientIPFrom(ctx context.Context) string {
	if s := ScopeFrom(ctx); s != nil && s.ClientIP != "" {
		return s.ClientIP
	}
	ip, _ := ctx.Value(clientIPKeyT{}).(string)
	return ip
}

func FeaturesFrom(ctx context.Context) []string {
	if s := ScopeFrom(ctx); s != nil && len(s.Features) > 0 {
		return s.Features
	}
	f, _ := ctx.Value(featuresKeyT{}).([]string)
	return f
}

func PermissionsFrom(ctx context.Context) []string {
	if s := ScopeFrom(ctx); s != nil && len(s.Permissions) > 0 {
		return s.Permissions
	}
	p, _ := ctx.Value(permissionsKeyT{}).([]string)
	return p
}

func HasRole(ctx context.Context, required string) bool {
	if s := ScopeFrom(ctx); s != nil {
		if s.Role == required {
			return true
		}
		return slices.Contains(s.Roles, required)
	}
	if r, ok := ctx.Value(roleKeyT{}).(string); ok {
		return r == required
	}
	if rs, ok := ctx.Value(roleKeyT{}).([]string); ok {
		return slices.Contains(rs, required)
	}
	return false
}

func HasAnyRole(ctx context.Context, allowed ...string) bool {
	for _, r := range allowed {
		if HasRole(ctx, r) {
			return true
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
	if perms, ok := ctx.Value(permissionsKeyT{}).([]string); ok {
		for _, p := range perms {
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
	for _, f := range FeaturesFrom(ctx) {
		if f == feature || f == "all" || f == "*" {
			return true
		}
	}
	return false
}

func CloneForAsync(ctx context.Context) context.Context {
	s := ScopeFrom(ctx)
	if s == nil {
		return context.WithoutCancel(ctx)
	}

	clone := &RequestScope{
		RequestID: s.RequestID,
		Endpoint:  s.Endpoint,
		UserID:    s.UserID,
		TenantID:  s.TenantID,
		TraceID:   s.TraceID,
		Role:      s.Role,
		ClientIP:  s.ClientIP,
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
