package ktest_test

import (
	"testing"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestCtx_ReturnsUsableScope(t *testing.T) {
	ctx, scope := ktest.Ctx(t)
	if scope == nil {
		t.Fatal("nil scope")
	}
	if xctx.ScopeFrom(ctx) != scope {
		t.Fatal("scope not bound to returned context")
	}
}

func TestCtxWithRole(t *testing.T) {
	ctx := ktest.CtxWithRole(t, "admin")
	if !xctx.HasRole(ctx, "admin") {
		t.Fatal("role not visible via HasRole")
	}
}

func TestCtxWithPerm(t *testing.T) {
	ctx := ktest.CtxWithPerm(t, "invoice:write")
	if !xctx.HasPermission(ctx, "invoice:write") {
		t.Fatal("permission not visible")
	}
}

func TestCtxWithTenant(t *testing.T) {
	ctx := ktest.CtxWithTenant(t, "acme")
	if xctx.TenantIDFrom(ctx) != "acme" {
		t.Fatalf("tenant = %q", xctx.TenantIDFrom(ctx))
	}
}

func TestCtxWithUser(t *testing.T) {
	ctx := ktest.CtxWithUser(t, "usr-1")
	if xctx.UserIDFrom(ctx) != "usr-1" {
		t.Fatalf("user = %q", xctx.UserIDFrom(ctx))
	}
}

func TestCtxWithAuth_Composite(t *testing.T) {
	ctx := ktest.CtxWithAuth(t, "u1", "t1", "operator", "auditor")
	if xctx.UserIDFrom(ctx) != "u1" {
		t.Fatal("user")
	}
	if xctx.TenantIDFrom(ctx) != "t1" {
		t.Fatal("tenant")
	}
	if !xctx.HasRole(ctx, "operator") || !xctx.HasRole(ctx, "auditor") {
		t.Fatal("roles")
	}
}
