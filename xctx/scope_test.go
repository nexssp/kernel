package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

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

func TestNilContext_Safety(t *testing.T) {
	t.Parallel()
	// None of these should panic. They must fallback cleanly.
	var ctx context.Context

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

func TestRequestScopeAllFields(t *testing.T) {
	ctx, scope, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	ctx = xctx.WithRequestID(ctx, "r1")
	ctx = xctx.WithExecutionID(ctx, "e1")
	ctx = xctx.WithTraceID(ctx, "t1")
	ctx = xctx.WithSpanID(ctx, "s1")
	ctx = xctx.WithTenantID(ctx, "ten1")
	ctx = xctx.WithUserID(ctx, "u1")

	if xctx.RequestIDFrom(ctx) != "r1" || scope.RequestID != "r1" {
		t.Fail()
	}
	if xctx.ExecutionIDFrom(ctx) != "e1" || scope.ExecutionID != "e1" {
		t.Fail()
	}
	if xctx.TraceIDFrom(ctx) != "t1" || scope.TraceID != "t1" {
		t.Fail()
	}
	if xctx.SpanIDFrom(ctx) != "s1" || scope.SpanID != "s1" {
		t.Fail()
	}
	if xctx.TenantIDFrom(ctx) != "ten1" || scope.TenantID != "ten1" {
		t.Fail()
	}
	if xctx.UserIDFrom(ctx) != "u1" || scope.UserID != "u1" {
		t.Fail()
	}

	// Test async cloning
	asyncCtx := xctx.CloneForAsync(ctx)
	if xctx.ExecutionIDFrom(asyncCtx) != "e1" {
		t.Errorf("expected execution ID preserved across async boundary")
	}
	if xctx.SpanIDFrom(asyncCtx) != "s1" {
		t.Errorf("expected span ID preserved across async boundary")
	}
}

func BenchmarkScopeReads(b *testing.B) {
	ctx, _, cleanup := xctx.NewScope(context.Background())
	defer cleanup()

	ctx = xctx.WithExecutionID(ctx, "exec-benchmark")
	ctx = xctx.WithTraceID(ctx, "trace-benchmark")

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		_ = xctx.ExecutionIDFrom(ctx)
		_ = xctx.TraceIDFrom(ctx)
	}
}

func TestRolesFrom_MissingScopeReturnsNil(t *testing.T) {
	t.Parallel()
	if got := xctx.RolesFrom(context.Background()); got != nil {
		t.Fatalf("RolesFrom on scope-less ctx = %v, want nil", got)
	}
}

func TestRolesFrom_IsSnapshot(t *testing.T) {
	t.Parallel()
	ctx, scope, cleanup := xctx.NewScope(context.Background())
	defer cleanup()
	ctx = xctx.WithRoles(ctx, []string{"admin", "auditor"})

	view := xctx.RolesFrom(ctx)
	view[0] = "hacker"

	if scope.Roles[0] != "admin" {
		t.Fatal("RolesFrom leaked a mutable view into the scope")
	}
	if xctx.RolesFrom(ctx)[0] != "admin" {
		t.Fatal("RolesFrom returned mutated storage")
	}
}
