// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Regression test for the correctness audit fix: ScopeFrom (and every
// getter built on it) now verifies the generation stored in the context.
// A context that outlives its request and holds a scope that the pool has
// rebound to a DIFFERENT request must fail closed (return nil / zero
// values) instead of reading the new request's identity.

package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestScopeFrom_StaleContextAfterRecycle_FailsClosed(t *testing.T) {
	t.Parallel()

	// Request A binds a scope and its identity.
	ctxA, scopeA, cleanupA := xctx.NewScope(context.Background())
	xctx.WithUserID(ctxA, "user-A")
	xctx.WithTenantID(ctxA, "tenant-A")
	xctx.WithPermissions(ctxA, []string{"read", "write"})

	if got := xctx.UserIDFrom(ctxA); got != "user-A" {
		t.Fatalf("expected user-A, got %q", got)
	}

	// Return the scope to the pool...
	cleanupA()

	// ...and let request B reuse it (sync.Pool very likely hands back the
	// same object; the test is only meaningful if it actually did — force
	// determinism by binding the SAME scope pointer to a new generation).
	ctxB, scopeB, cleanupB := xctx.NewScope(context.Background())
	if scopeB != scopeA {
		// Different object came back — rebind A's scope manually to
		// simulate the recycle deterministically.
		cleanupB()
		ctxB = xctx.WithScope(context.Background(), scopeA)
	}
	xctx.WithUserID(ctxB, "user-B")
	xctx.WithTenantID(ctxB, "tenant-B")

	if got := xctx.UserIDFrom(ctxB); got != "user-B" {
		t.Fatalf("expected user-B from live context, got %q", got)
	}

	// THE FIX: A's stale context must NOT observe B's identity. Before the
	// generation guard, every getter below returned B's data.
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

	// The live context is unaffected by the stale one.
	if got := xctx.UserIDFrom(ctxB); got != "user-B" {
		t.Fatalf("live context corrupted by stale read: %q", got)
	}

	cleanupB()
}

// AddTrace from a stale context must not corrupt the recycled scope's trace
// ring (the write path was already guarded; this pins the contract now that
// reads are guarded too).
func TestAddTrace_FromStaleContext_Ignored(t *testing.T) {
	t.Parallel()

	ctxA, scopeA, cleanupA := xctx.NewScope(context.Background())
	cleanupA()

	ctxLive := xctx.WithScope(context.Background(), scopeA)
	xctx.AddTrace(ctxLive, "live-event")

	// Stale write — before the write-guard era this corrupted the new owner.
	xctx.AddTrace(ctxA, "stale-event")

	// AddTrace from a stale context must not corrupt the recycled scope's
	// trace ring: read it back through the guarded ScopeFrom on the live ctx.
	s := xctx.ScopeFrom(ctxLive)
	if s == nil {
		t.Fatal("live context must resolve its scope")
	}
	if len(s.TraceEvents) != 1 || s.TraceEvents[0] != "live-event" {
		t.Fatalf("trace ring corrupted via stale context: %v", s.TraceEvents)
	}
}
