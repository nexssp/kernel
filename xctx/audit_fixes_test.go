// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestScopeFrom_StaleContextAfterRecycle_FailsClosed(t *testing.T) {
	t.Parallel()

	scopeA := &xctx.RequestScope{}
	ctxA := xctx.WithScope(context.Background(), scopeA)
	xctx.WithUserID(ctxA, "user-A")
	xctx.WithTenantID(ctxA, "tenant-A")
	xctx.WithPermissions(ctxA, []string{"read", "write"})

	if got := xctx.UserIDFrom(ctxA); got != "user-A" {
		t.Fatalf("expected user-A, got %q", got)
	}

	scopeA.Reset()

	ctxB := xctx.WithScope(context.Background(), scopeA)
	xctx.WithUserID(ctxB, "user-B")
	xctx.WithTenantID(ctxB, "tenant-B")

	if got := xctx.UserIDFrom(ctxB); got != "user-B" {
		t.Fatalf("expected user-B from live context, got %q", got)
	}

	if got := xctx.UserIDFrom(ctxA); got != "" {
		t.Fatalf("stale context leaked live identity: UserID=%q", got)
	}
	if got := xctx.TenantIDFrom(ctxA); got != "" {
		t.Fatalf("stale context leaked live identity: TenantID=%q", got)
	}
	if got := xctx.PermissionsFrom(ctxA); len(got) != 0 {
		t.Fatalf("stale context leaked live identity: Permissions=%v", got)
	}
	if s := xctx.ScopeFrom(ctxA); s != nil {
		t.Fatal("stale context must not resolve a scope")
	}

	if got := xctx.UserIDFrom(ctxB); got != "user-B" {
		t.Fatalf("live context corrupted by stale read: %q", got)
	}
}

func TestAddTrace_FromStaleContext_Ignored(t *testing.T) {
	t.Parallel()

	scopeA := &xctx.RequestScope{}
	ctxA := xctx.WithScope(context.Background(), scopeA)

	scopeA.Reset()

	ctxLive := xctx.WithScope(context.Background(), scopeA)
	xctx.AddTrace(ctxLive, "live-event")

	xctx.AddTrace(ctxA, "stale-event")

	s := xctx.ScopeFrom(ctxLive)
	if s == nil {
		t.Fatal("live context must resolve its scope")
	}
	if len(s.TraceEvents) != 1 || s.TraceEvents[0] != "live-event" {
		t.Fatalf("trace ring corrupted via stale context: %v", s.TraceEvents)
	}
}
