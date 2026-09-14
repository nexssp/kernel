package xctx_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/xctx"
)

type mockPublisher struct {
	lastSubject string
}

func (m *mockPublisher) PublishEvent(_ context.Context, subject string, _ any) error {
	m.lastSubject = subject
	return nil
}

func TestScopeFrom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if s := xctx.ScopeFrom(ctx); s != nil {
		t.Fatal("expected nil scope initially")
	}

	scope := &xctx.RequestScope{RequestID: "req-1"}
	ctx = xctx.WithScope(ctx, scope)

	if s := xctx.ScopeFrom(ctx); s != scope {
		t.Fatal("expected exact scope instance from context")
	}
}

func TestNewScopeLifecycleAndPooling(t *testing.T) {
	t.Parallel()
	parent := context.Background()

	ctx, scope, release := xctx.NewScope(parent)
	if scope == nil {
		t.Fatal("expected non-nil RequestScope")
	}

	scope.UserID = "user-777"
	scope.Roles = append(scope.Roles, "admin")
	xctx.AddTrace(ctx, "started_processing")

	if xctx.UserIDFrom(ctx) != "user-777" {
		t.Fatalf("expected UserID 'user-777', got %q", xctx.UserIDFrom(ctx))
	}
	if len(scope.TraceEvents) != 1 {
		t.Fatalf("expected 1 trace event, got %d", len(scope.TraceEvents))
	}

	// Return to sync.Pool and verify reset
	release()

	if scope.UserID != "" || len(scope.Roles) != 0 || len(scope.TraceEvents) != 0 {
		t.Fatal("expected scope fields to be completely reset after release")
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

func TestCloneForAsync(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	ctx, scope, release := xctx.NewScope(ctx)
	scope.UserID = "usr_async"
	scope.Roles = []string{"admin"}

	asyncCtx := xctx.CloneForAsync(ctx)
	release() // Release parent scope back to pool immediately

	// Parent context times out
	time.Sleep(15 * time.Millisecond)
	if ctx.Err() == nil {
		t.Fatal("expected parent context to be canceled")
	}

	// Async context remains uncancelled and retains cloned data safely
	if asyncCtx.Err() != nil {
		t.Fatal("expected async context to remain active without cancellation")
	}
	if xctx.UserIDFrom(asyncCtx) != "usr_async" {
		t.Fatalf("expected UserID 'usr_async' in async context, got %q", xctx.UserIDFrom(asyncCtx))
	}
}

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

func TestEventPublisher(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if _, ok := xctx.EventPublisherFrom(ctx); ok {
		t.Fatal("expected EventPublisher to be missing initially")
	}

	pub := &mockPublisher{}
	ctx = xctx.WithEventPublisher(ctx, pub)

	retrieved, ok := xctx.EventPublisherFrom(ctx)
	if !ok || retrieved == nil {
		t.Fatal("expected EventPublisher in context")
	}

	_ = retrieved.PublishEvent(ctx, "order.created", nil)
	if pub.lastSubject != "order.created" {
		t.Fatalf("expected subject 'order.created', got %q", pub.lastSubject)
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
		// and MUST be 100% zero-alloc
		ctx = xctx.WithUserID(ctx, "user-123")
		ctx = xctx.WithTenantID(ctx, "tenant-456")
		ctx = xctx.WithRequestID(ctx, "req-789")
		ctx = xctx.WithTraceID(ctx, "trace-abc")
		ctx = xctx.WithEndpoint(ctx, "/api")
		ctx = xctx.WithClientIP(ctx, "127.0.0.1")

		// Assigning slices should reuse pre-warmed capacity
		ctx = xctx.WithRoles(ctx, roles)
		ctx = xctx.WithFeatures(ctx, features)
		ctx = xctx.WithPermissions(ctx, perms)
	})

	if allocs != 0 {
		t.Fatalf("CRITICAL: expected exactly 0 allocations when mutating scope, got %.2f", allocs)
	}
}

func TestNilContext_Safety(t *testing.T) {
	t.Parallel()
	// None of these should panic. They must fallback cleanly.
	var ctx context.Context = nil

	ctx = xctx.WithUserID(ctx, "safe_fallback")
	if xctx.UserIDFrom(ctx) != "safe_fallback" {
		t.Fatal("expected lazy fallback scope to retain UserID")
	}

	ctx2, scope, release := xctx.NewScope(nil) //nolint:staticcheck // nil context edge case
	defer release()
	if ctx2 == nil || scope == nil {
		t.Fatal("NewScope(nil) failed to fallback to context.Background()")
	}

	clone := xctx.CloneForAsync(nil) //nolint:staticcheck // nil context edge case
	if clone == nil {
		t.Fatal("CloneForAsync(nil) failed to return a valid context")
	}

	if _, ok := xctx.NewKey[int]("nil.test").From(nil); ok { //nolint:staticcheck // nil context edge case
		t.Fatal("Key.From(nil) should safely return false")
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

func TestTypedKey(t *testing.T) {
	t.Parallel()
	key := xctx.NewKey[int]("test.int")

	ctx := key.With(context.Background(), 42)

	val, ok := key.From(ctx)
	if !ok || val != 42 {
		t.Fatalf("expected 42, got %v", val)
	}

	if val := key.MustFrom(ctx); val != 42 {
		t.Fatalf("MustFrom expected 42, got %v", val)
	}
}

func TestTypedKey_MustFromPanic(t *testing.T) {
	t.Parallel()
	key := xctx.NewKey[string]("test.missing")

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected MustFrom to panic on missing key")
		}
	}()

	_ = key.MustFrom(context.Background())
}
