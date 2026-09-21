package ktest

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

// RequestContext returns the test context carrying requestID. The request ID
// is the idempotency key used by action middleware; cancellation and timeout
// remain explicit with context.WithCancel/WithTimeout at the call site.
func RequestContext(tb testing.TB, requestID string) context.Context {
	tb.Helper()
	return xctx.WithRequestID(tb.Context(), requestID)
}

// Ctx returns a fresh request scope bound to context.Background() and
// registered with tb.Cleanup so it is released back to the pool when the
// test ends.
func Ctx(tb testing.TB) (context.Context, *xctx.RequestScope) {
	tb.Helper()
	ctx, scope, release := xctx.NewScope(context.Background())
	tb.Cleanup(release)
	return ctx, scope
}

// CtxWithRole returns a context carrying a scope with the given role.
func CtxWithRole(tb testing.TB, role string) context.Context {
	tb.Helper()
	ctx, _ := Ctx(tb)
	return xctx.WithRoles(ctx, []string{role})
}

// CtxWithPerm returns a context carrying a scope with the given permission.
func CtxWithPerm(tb testing.TB, perm string) context.Context {
	tb.Helper()
	ctx, _ := Ctx(tb)
	return xctx.WithPermissions(ctx, []string{perm})
}

// CtxWithTenant returns a context carrying a scope with the tenant ID.
func CtxWithTenant(tb testing.TB, tenantID string) context.Context {
	tb.Helper()
	ctx, _ := Ctx(tb)
	return xctx.WithTenantID(ctx, tenantID)
}

// CtxWithUser returns a context carrying a scope with the user ID.
func CtxWithUser(tb testing.TB, userID string) context.Context {
	tb.Helper()
	ctx, _ := Ctx(tb)
	return xctx.WithUserID(ctx, userID)
}

// CtxWithAuth is the common composite: user + tenant + roles in a single call.
func CtxWithAuth(tb testing.TB, userID, tenantID string, roles ...string) context.Context {
	tb.Helper()
	ctx, _ := Ctx(tb)
	ctx = xctx.WithUserID(ctx, userID)
	ctx = xctx.WithTenantID(ctx, tenantID)
	if len(roles) > 0 {
		ctx = xctx.WithRoles(ctx, roles)
	}
	return ctx
}
