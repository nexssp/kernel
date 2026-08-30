package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

func TestRequireRole(t *testing.T) {
	t.Parallel()

	act := action.New("admin.only", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireRole("admin").Build()

	// 1. Without Role
	_, err := act.Do(context.Background(), struct{}{})
	if err == nil {
		t.Fatal("expected forbidden error, got nil")
	}

	// 2. With Role
	ctx := xctx.WithRoles(context.Background(), []string{"admin"})
	res, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "ok" {
		t.Fatalf("expected ok, got %v", res)
	}
}

func TestRequireFeature(t *testing.T) {
	t.Parallel()

	act := action.New("beta.feature", func(ctx context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).RequireFeature("beta-ui").Build()

	ctx := xctx.WithFeatures(context.Background(), []string{"beta-ui"})
	_, err := act.Do(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
