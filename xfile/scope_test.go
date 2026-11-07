package xfile_test

import (
	"context"
	"sync"
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := xfile.ScopeFromContext(test.ctx)
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

func TestScopeFromContextDoesNotMixConcurrentIdentityUpdates(t *testing.T) {
	ctx := newInterleavingScopeContext()
	ctx.updateOnScopeLookup = 2
	defer ctx.finish()

	got, err := xfile.ScopeFromContext(ctx)
	if err != nil {
		t.Fatalf("ScopeFromContext() error = %v", err)
	}
	if got.CallerID != "caller-old" || got.TenantID != "tenant-old" {
		t.Fatalf("ScopeFromContext() mixed identity versions: %#v", got)
	}
}

func TestSeparateScopeReadsCanMixConcurrentIdentityUpdates(t *testing.T) {
	ctx := newInterleavingScopeContext()
	ctx.updateOnScopeLookup = 2
	defer ctx.finish()

	callerID := xctx.UserIDFrom(ctx)
	ctx.triggerUpdate()
	tenantID := xctx.TenantIDFrom(ctx)
	if callerID != "caller-old" || tenantID != "tenant-new" {
		t.Fatalf("separate scope reads = (%q, %q), want forced mixed identity", callerID, tenantID)
	}
}

func newInterleavingScopeContext() *interleavingScopeContext {
	base := xctx.WithTenantID(context.Background(), "tenant-old")
	base = xctx.WithUserID(base, "caller-old")
	ctx := &interleavingScopeContext{
		Context:     base,
		scope:       xctx.ScopeFrom(base),
		startUpdate: make(chan struct{}),
		updated:     make(chan struct{}),
	}
	go func() {
		<-ctx.startUpdate
		xctx.FromClaims(ctx.scope, map[string]any{"sub": "caller-new", "ten": "tenant-new"})
		close(ctx.updated)
	}()
	return ctx
}

type interleavingScopeContext struct {
	context.Context
	scope               *xctx.RequestScope
	startUpdate         chan struct{}
	updated             chan struct{}
	updateOnce          sync.Once
	updateOnScopeLookup int
	scopeLookups        int
}

func (ctx *interleavingScopeContext) Value(key any) any {
	value := ctx.Context.Value(key)
	if _, ok := value.(*xctx.RequestScope); ok {
		ctx.scopeLookups++
		if ctx.scopeLookups == ctx.updateOnScopeLookup {
			ctx.triggerUpdate()
		}
	}
	return value
}

func (ctx *interleavingScopeContext) finish() {
	ctx.triggerUpdate()
}

func (ctx *interleavingScopeContext) triggerUpdate() {
	ctx.updateOnce.Do(func() { close(ctx.startUpdate) })
	<-ctx.updated
}
