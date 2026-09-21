package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestRequireAuth(t *testing.T) {
	t.Parallel()
	act := action.New("auth.only", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireAuth().Build()

	_, err := act.Do(context.Background(), struct{}{})
	ktest.RequireErrorKind(t, err, xerr.KindUnauthorized)

	res, err := act.Do(ktest.CtxWithUser(t, "user-123"), struct{}{})
	if err != nil || res != "ok" {
		t.Fatalf("expected 'ok', got res=%v, err=%v", res, err)
	}
}

func TestRequireTenant(t *testing.T) {
	t.Parallel()
	act := action.New("tenant.only", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireTenant().Build()

	_, err := act.Do(context.Background(), struct{}{})
	ktest.RequireErrorKind(t, err, xerr.KindUnauthorized)

	res, err := act.Do(ktest.CtxWithTenant(t, "t-1"), struct{}{})
	if err != nil || res != "ok" {
		t.Fatalf("expected 'ok', got res=%v, err=%v", res, err)
	}
}

func TestRequireAnyRole(t *testing.T) {
	t.Parallel()
	act := action.New("any.role", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireAnyRole("admin", "moderator").Build()

	_, err := act.Do(context.Background(), struct{}{})
	ktest.RequireErrorKind(t, err, xerr.KindForbidden)

	res, err := act.Do(ktest.CtxWithRole(t, "moderator"), struct{}{})
	if err != nil || res != "ok" {
		t.Fatalf("expected 'ok', got res=%v, err=%v", res, err)
	}
}

func TestRequirePermission(t *testing.T) {
	t.Parallel()
	act := action.New("perm.only", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequirePermission("write:orders").Build()

	_, err := act.Do(context.Background(), struct{}{})
	ktest.RequireErrorKind(t, err, xerr.KindForbidden)

	res, err := act.Do(ktest.CtxWithPerm(t, "write:orders"), struct{}{})
	if err != nil || res != "ok" {
		t.Fatalf("expected 'ok', got res=%v, err=%v", res, err)
	}
}

func TestRequireAnyFeature(t *testing.T) {
	t.Parallel()
	act := action.New("any.feat", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireAnyFeature("beta", "premium").Build()

	_, err := act.Do(context.Background(), struct{}{})
	ktest.RequireErrorKind(t, err, xerr.KindForbidden)

	ctx, scope := ktest.Ctx(t)
	scope.Features = []string{"beta"}

	res, err := act.Do(ctx, struct{}{})
	if err != nil || res != "ok" {
		t.Fatalf("expected 'ok', got res=%v, err=%v", res, err)
	}
}
