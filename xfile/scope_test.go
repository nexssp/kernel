package xfile_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xfile"
)

func TestScopeFromContextRequiresCallerAndTenant(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
	}{
		{name: "no request scope", ctx: context.Background()},
		{name: "no caller", ctx: xctx.WithTenantID(context.Background(), "tenant-1")},
		{name: "no tenant", ctx: xctx.WithUserID(context.Background(), "caller-1")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := xfile.ScopeFromContext(tt.ctx)
			if err == nil {
				t.Fatalf("ScopeFromContext() = %#v, nil; want an authorization error", got)
			}
			if kind := xerr.KindFrom(err); kind != xerr.KindUnauthorized {
				t.Fatalf("error kind = %q; want %q", kind, xerr.KindUnauthorized)
			}
		})
	}
}

func TestScopeFromContextCopiesAuthenticatedCallerAndTenant(t *testing.T) {
	ctx := xctx.WithTenantID(context.Background(), "tenant-1")
	ctx = xctx.WithUserID(ctx, "caller-1")

	got, err := xfile.ScopeFromContext(ctx)
	if err != nil {
		t.Fatalf("ScopeFromContext() error = %v", err)
	}
	if got.CallerID != "caller-1" || got.TenantID != "tenant-1" {
		t.Fatalf("ScopeFromContext() = %#v", got)
	}
}
