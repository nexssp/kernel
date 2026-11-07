package xctx_test

import (
	"context"
	"sync"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestSettersAndGetters(t *testing.T) {
	t.Parallel()
	ctx, _, release := xctx.NewScope(context.Background())
	defer release()

	ctx = xctx.WithRequestID(ctx, "req-999")
	ctx = xctx.WithEndpoint(ctx, "/api/v1/test")
	ctx = xctx.WithTenantID(ctx, "tenant-999")
	ctx = xctx.WithTraceID(ctx, "trace-999")
	ctx = xctx.WithClientIP(ctx, "192.168.1.1")

	if xctx.RequestIDFrom(ctx) != "req-999" {
		t.Fatalf("expected RequestID 'req-999', got %q", xctx.RequestIDFrom(ctx))
	}
	if xctx.EndpointFrom(ctx) != "/api/v1/test" {
		t.Fatalf("expected Endpoint '/api/v1/test', got %q", xctx.EndpointFrom(ctx))
	}
	if xctx.TenantIDFrom(ctx) != "tenant-999" {
		t.Fatalf("expected TenantID 'tenant-999', got %q", xctx.TenantIDFrom(ctx))
	}
	if xctx.TraceIDFrom(ctx) != "trace-999" {
		t.Fatalf("expected TraceID 'trace-999', got %q", xctx.TraceIDFrom(ctx))
	}
	if xctx.ClientIPFrom(ctx) != "192.168.1.1" {
		t.Fatalf("expected ClientIP '192.168.1.1', got %q", xctx.ClientIPFrom(ctx))
	}
}

func TestFromClaims(t *testing.T) {
	t.Parallel()
	scope := &xctx.RequestScope{}

	claims := map[string]any{
		"sub":      "usr_100",
		"ten":      "tenant_abc",
		"jti":      "jwt_id_123",
		"roles":    []any{"operator", "viewer"},
		"features": []any{"ksef", "ai"},
		"perms":    []any{"invoice:create"},
	}

	xctx.FromClaims(scope, claims)

	if scope.UserID != "usr_100" {
		t.Fatalf("expected UserID 'usr_100', got %q", scope.UserID)
	}
	if scope.TenantID != "tenant_abc" {
		t.Fatalf("expected TenantID 'tenant_abc', got %q", scope.TenantID)
	}
	if scope.Role != "operator" {
		t.Fatalf("expected primary Role 'operator', got %q", scope.Role)
	}
	if !xctx.HasFeature(xctx.WithScope(context.Background(), scope), "ksef") {
		t.Fatal("expected feature 'ksef' to be active")
	}
	if !xctx.HasPermission(xctx.WithScope(context.Background(), scope), "invoice:create") {
		t.Fatal("expected permission 'invoice:create' to be active")
	}
}

func TestZeroAlloc_SettersWithScope(t *testing.T) {
	ctx, _, release := xctx.NewScope(context.Background())
	defer release()

	roles := []string{"admin", "editor"}
	features := []string{"beta", "ai"}
	perms := []string{"read", "write"}

	// Pre-warm the scope so internal slices grow to required capacity
	ctx = xctx.WithRoles(ctx, roles)
	ctx = xctx.WithFeatures(ctx, features)
	ctx = xctx.WithPermissions(ctx, perms)

	allocs := testing.AllocsPerRun(1000, func() {
		// Mutating an existing RequestScope must not trigger context.WithValue
		// and MUST be 100% zero-alloc.
		c := ctx
		c = xctx.WithUserID(c, "user-123")
		c = xctx.WithTenantID(c, "tenant-456")
		c = xctx.WithRequestID(c, "req-789")
		c = xctx.WithTraceID(c, "trace-abc")
		c = xctx.WithEndpoint(c, "/api")
		c = xctx.WithClientIP(c, "127.0.0.1")
		c = xctx.WithRoles(c, roles)
		c = xctx.WithFeatures(c, features)
		c = xctx.WithPermissions(c, perms)

		if !xctx.HasPermission(c, "write") {
			t.Fatal("expected 'write' permission to be set")
		}
	})

	if allocs != 0 {
		t.Fatalf("CRITICAL: expected exactly 0 allocations when mutating scope, got %.2f", allocs)
	}
}

func TestSliceOwnershipAndMutation(t *testing.T) {
	t.Parallel()
	ctx, _, release := xctx.NewScope(context.Background())
	defer release()

	callerRoles := []string{"admin", "editor"}
	ctx = xctx.WithRoles(ctx, callerRoles)

	// Caller maliciously or accidentally mutates the slice
	callerRoles[0] = "hacker"

	// Ensure context was protected by defensive copy
	if !xctx.HasRole(ctx, "admin") {
		t.Fatal("context lost 'admin' role after caller mutation")
	}
	if xctx.HasRole(ctx, "hacker") {
		t.Fatal("context state was corrupted by caller slice mutation")
	}
}

func TestWithRoles_Resetting(t *testing.T) {
	t.Parallel()
	ctx, scope, release := xctx.NewScope(context.Background())
	defer release()

	ctx = xctx.WithRoles(ctx, []string{"operator"})
	if scope.Role != "operator" || !xctx.HasRole(ctx, "operator") {
		t.Fatal("failed to set initial role")
	}

	// Clearing roles should clear both the slice and the primary Role field
	ctx = xctx.WithRoles(ctx, nil)
	if scope.Role != "" {
		t.Fatalf("expected primary role to be cleared, got %q", scope.Role)
	}
	if xctx.HasRole(ctx, "operator") {
		t.Fatal("HasRole returned true after roles were cleared")
	}
}

func TestScopeAccessorsConcurrentMutationAndRead(t *testing.T) {
	t.Parallel()
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithUserID(ctx, "initial")
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		<-start
		for range 10_000 {
			_ = xctx.WithUserID(ctx, "user")
		}
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		for range 10_000 {
			_ = xctx.UserIDFrom(ctx)
		}
	}()
	close(start)
	waitGroup.Wait()
}

func TestSnapshotFromCopiesMetadataAndSlices(t *testing.T) {
	t.Parallel()
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithRequestID(ctx, "request-1")
	ctx = xctx.WithEndpoint(ctx, "/checkout")
	ctx = xctx.WithUserID(ctx, "user-1")
	ctx = xctx.WithTenantID(ctx, "tenant-1")
	ctx = xctx.WithTraceID(ctx, "trace-1")
	ctx = xctx.WithSpanID(ctx, "span-1")
	ctx = xctx.WithClientIP(ctx, "127.0.0.1")
	ctx = xctx.WithRoles(ctx, []string{"admin", "billing"})
	ctx = xctx.WithPermissions(ctx, []string{"read", "*"})
	ctx = xctx.WithFeatures(ctx, []string{"beta", "*"})

	snapshot := xctx.SnapshotFrom(ctx)
	if !snapshot.Present || snapshot.RequestID != "request-1" || snapshot.Endpoint != "/checkout" ||
		snapshot.UserID != "user-1" || snapshot.TenantID != "tenant-1" || snapshot.TraceID != "trace-1" ||
		snapshot.SpanID != "span-1" || snapshot.ClientIP != "127.0.0.1" {
		t.Fatalf("snapshot identity fields = %+v", snapshot)
	}
	if !snapshot.HasRole("billing") || !snapshot.HasPermission("write") || !snapshot.HasFeature("beta") || !snapshot.HasFeature("arbitrary") {
		t.Fatal("snapshot authorization helpers did not preserve scope semantics")
	}
	adminSnapshot := xctx.ScopeSnapshot{Present: true, Roles: []string{"system_admin"}}
	if !adminSnapshot.HasFeature("arbitrary") || (xctx.ScopeSnapshot{}).HasRole("") {
		t.Fatal("snapshot admin or absent-scope semantics are incorrect")
	}

	ctx = xctx.WithUserID(ctx, "user-2")
	ctx = xctx.WithRoles(ctx, []string{"viewer"})
	ctx = xctx.WithPermissions(ctx, []string{"other"})
	ctx = xctx.WithFeatures(ctx, []string{"other"})
	if snapshot.UserID != "user-1" || snapshot.Roles[0] != "admin" || snapshot.Permissions[0] != "read" ||
		snapshot.Features[0] != "beta" {
		t.Fatalf("scope mutation changed snapshot: %+v", snapshot)
	}

	snapshot.Roles[0] = "snapshot-mutated"
	snapshot.Permissions[0] = "snapshot-mutated"
	snapshot.Features[0] = "snapshot-mutated"
	current := xctx.SnapshotFrom(ctx)
	if current.Roles[0] != "viewer" || current.Permissions[0] != "other" || current.Features[0] != "other" {
		t.Fatalf("snapshot mutation changed scope: %+v", current)
	}
}

func TestSnapshotFromMissingOrStaleScope(t *testing.T) {
	t.Parallel()
	if snapshot := xctx.SnapshotFrom(context.Background()); snapshot.Present {
		t.Fatal("snapshot of context without a scope is marked present")
	}
	ctx, scope, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithUserID(ctx, "stale-user")
	scope.Reset()
	if snapshot := xctx.SnapshotFrom(ctx); snapshot.Present || snapshot.UserID != "" {
		t.Fatalf("snapshot of stale scope = %+v", snapshot)
	}
}

func TestScopeReadDuringResetIsRaceFree(t *testing.T) {
	t.Parallel()
	ctx, scope, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithUserID(ctx, "user-1")
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		<-start
		for range 10_000 {
			_ = xctx.UserIDFrom(ctx)
			_ = xctx.SnapshotFrom(ctx)
		}
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		for range 100 {
			scope.Reset()
		}
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		for range 100 {
			xctx.AddTrace(ctx, "event")
		}
	}()
	close(start)
	waitGroup.Wait()
	if xctx.SnapshotFrom(ctx).Present {
		t.Fatal("reset scope remained bound to its old context")
	}
}

var (
	benchmarkUserID        string
	benchmarkPermissionOK  bool
	benchmarkTenantID      string
	benchmarkTraceID       string
	benchmarkSpanID        string
	benchmarkRoleOK        bool
	benchmarkScopeSnapshot xctx.ScopeSnapshot
)

func BenchmarkUserIDFrom(b *testing.B) {
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithUserID(ctx, "user-1")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkUserID = xctx.UserIDFrom(ctx)
	}
}

func BenchmarkHasPermission(b *testing.B) {
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithPermissions(ctx, []string{"read:profile", "write:profile"})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkPermissionOK = xctx.HasPermission(ctx, "write:profile")
	}
}

func BenchmarkScopeReadSixFields(b *testing.B) {
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithRequestID(ctx, "request-1")
	ctx = xctx.WithEndpoint(ctx, "/checkout")
	ctx = xctx.WithUserID(ctx, "user-1")
	ctx = xctx.WithTenantID(ctx, "tenant-1")
	ctx = xctx.WithTraceID(ctx, "trace-1")
	ctx = xctx.WithSpanID(ctx, "span-1")
	ctx = xctx.WithClientIP(ctx, "127.0.0.1")
	ctx = xctx.WithRoles(ctx, []string{"admin", "billing"})
	ctx = xctx.WithPermissions(ctx, []string{"read", "write"})
	ctx = xctx.WithFeatures(ctx, []string{"beta", "audit"})
	xctx.AddTrace(ctx, "benchmark-event")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkUserID = xctx.UserIDFrom(ctx)
		benchmarkTenantID = xctx.TenantIDFrom(ctx)
		benchmarkTraceID = xctx.TraceIDFrom(ctx)
		benchmarkSpanID = xctx.SpanIDFrom(ctx)
		benchmarkRoleOK = xctx.HasRole(ctx, "admin")
		benchmarkPermissionOK = xctx.HasPermission(ctx, "write")
	}
}

func BenchmarkScopeSnapshotSixFields(b *testing.B) {
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithRequestID(ctx, "request-1")
	ctx = xctx.WithEndpoint(ctx, "/checkout")
	ctx = xctx.WithUserID(ctx, "user-1")
	ctx = xctx.WithTenantID(ctx, "tenant-1")
	ctx = xctx.WithTraceID(ctx, "trace-1")
	ctx = xctx.WithSpanID(ctx, "span-1")
	ctx = xctx.WithClientIP(ctx, "127.0.0.1")
	ctx = xctx.WithRoles(ctx, []string{"admin", "billing"})
	ctx = xctx.WithPermissions(ctx, []string{"read", "write"})
	ctx = xctx.WithFeatures(ctx, []string{"beta", "audit"})
	xctx.AddTrace(ctx, "benchmark-event")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot := xctx.SnapshotFrom(ctx)
		benchmarkScopeSnapshot = snapshot
		benchmarkUserID = snapshot.UserID
		benchmarkTenantID = snapshot.TenantID
		benchmarkTraceID = snapshot.TraceID
		benchmarkSpanID = snapshot.SpanID
		benchmarkRoleOK = snapshot.HasRole("admin")
		benchmarkPermissionOK = snapshot.HasPermission("write")
	}
}
