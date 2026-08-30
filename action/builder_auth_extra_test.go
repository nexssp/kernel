package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

func TestRequireAuth(t *testing.T) {
	t.Parallel()
	act := action.New("auth.only", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireAuth().Build()

	// Without user ID → should fail
	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected unauthorized error, got nil")
	}

	// With user ID → should pass
	ctx := xctx.WithUserID(context.Background(), "user-123")
	res, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %v", res)
	}
}

func TestRequireTenant(t *testing.T) {
	t.Parallel()
	act := action.New("tenant.only", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireTenant().Build()

	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected unauthorized error, got nil")
	}

	ctx := xctx.WithTenantID(context.Background(), "t-1")
	res, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %v", res)
	}
}

func TestRequireAnyRole(t *testing.T) {
	t.Parallel()
	act := action.New("any.role", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireAnyRole("admin", "moderator").Build()

	// No role → fail
	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected forbidden error, got nil")
	}

	// Has matching role → pass
	ctx := xctx.WithRoles(context.Background(), []string{"moderator"})
	res, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %v", res)
	}
}

func TestRequirePermission(t *testing.T) {
	t.Parallel()
	act := action.New("perm.only", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequirePermission("write:orders").Build()

	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected forbidden error, got nil")
	}

	// FIX: Inject an empty Scope into the context first!
	ctx := xctx.WithScope(context.Background(), &xctx.RequestScope{})

	ctx = xctx.WithPermissions(ctx, []string{"write:orders"})
	res, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %v", res)
	}
}

func TestRequireAnyFeature(t *testing.T) {
	t.Parallel()
	act := action.New("any.feat", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireAnyFeature("beta", "premium").Build()

	// No feature → fail
	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected forbidden error, got nil")
	}

	// One matching feature → pass
	ctx := xctx.WithFeatures(context.Background(), []string{"beta"})
	res, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected 'ok', got %v", res)
	}
}
