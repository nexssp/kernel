package action

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/xctx"
)

func TestWithProfileAppliesCommandDefaults(t *testing.T) {
	act := New("profile.command", func(_ context.Context, value string) (string, error) { return value, nil }).
		WithProfile(IdempotentCommandProfile("orders:write", "orders", "command")).
		Timeout(5 * time.Second).
		Build()
	meta := act.GetMeta()
	if !meta.Idempotency.Enabled || meta.Timeout != 5*time.Second || !meta.RequiresAuth {
		t.Fatalf("profile metadata = %#v", meta)
	}
	if len(meta.RequiredPermissions) != 1 || meta.RequiredPermissions[0] != "orders:write" {
		t.Fatalf("profile permissions = %#v", meta.RequiredPermissions)
	}
	ctx := xctx.WithPermissions(context.Background(), []string{"orders:write"})
	got, err := act.Do(ctx, "ok")
	if err != nil || got != "ok" {
		t.Fatalf("profile dispatch = %q, %v", got, err)
	}
}

func TestInternalEventProfileRequiresIdentity(t *testing.T) {
	act := New("profile.event", func(_ context.Context, _ struct{}) (string, error) { return "ok", nil }).WithProfile(InternalEventProfile("internal")).Build()
	if _, err := act.Do(context.Background(), struct{}{}); err == nil {
		t.Fatal("internal event profile must require authenticated identity")
	}
}
