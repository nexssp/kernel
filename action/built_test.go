package action_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

type httpBinding struct {
	Method string
	Path   string
}

type cliBinding struct {
	Command string
}

// ── 1. Nil & Zero-Value Edge Cases ────────────────────────────────────────────

func TestBuiltAction_ToBuilder_NilReceiverSafety(t *testing.T) {
	t.Parallel()

	var nilAction *action.BuiltAction[string, string]
	b := nilAction.ToBuilder()
	if b != nil {
		t.Fatalf("expected nil builder from nil action, got %v", b)
	}
}

func TestBuiltAction_ToBuilder_MinimalZeroValueAction(t *testing.T) {
	t.Parallel()

	// An action with no hooks, no tags, no bindings, no middleware
	minAct := action.New("minimal", func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	}).Build()

	rebuilt := minAct.ToBuilder().Build()

	res, err := rebuilt.Do(context.Background(), 21)
	if err != nil || res != 42 {
		t.Fatalf("minimal action failed: res=%d, err=%v", res, err)
	}
	if len(rebuilt.GetBindings()) != 0 {
		t.Errorf("expected 0 bindings, got %d", len(rebuilt.GetBindings()))
	}
	if rebuilt.History() != nil {
		t.Errorf("expected nil history, got %+v", rebuilt.History())
	}
}

// ── 2. Execution Bad Paths & Error Propagation ────────────────────────────────

func TestBuiltAction_ToBuilder_ErrorPathAndHookPropagation(t *testing.T) {
	t.Parallel()

	var (
		typedErrorHookRan atomic.Bool
		anyErrorHookRan   atomic.Bool
		executedHookRan   atomic.Bool
		expectedErr       = errors.New("upstream service unavailable")
	)

	// Original action with error hooks and validation
	orig := action.New("failing.action", func(_ context.Context, _ string) (string, error) {
		return "", expectedErr
	}).
		Hook(action.Hook[string, string]{
			OnError: func(_ context.Context, _ string, _ error, _ *action.Meta) {
				typedErrorHookRan.Store(true)
			},
			OnExecuted: func(_ context.Context, _, _ string, _ error, _ *action.Meta) {
				executedHookRan.Store(true)
			},
		}).
		AnyHook(action.AnyHook{
			OnError: func(_ context.Context, _ any, _ error, _ *action.Meta) {
				anyErrorHookRan.Store(true)
			},
		}).
		Build()

	rebuilt := orig.ToBuilder().Build()

	// Bad path: Do() MUST return the exact error and trigger error hooks only
	_, err := rebuilt.Do(context.Background(), "input")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error chain to unwrap to %v, got %v", expectedErr, err)
	}
	if !typedErrorHookRan.Load() {
		t.Error("typed OnError hook was NOT fired on rebuilt action")
	}
	if !anyErrorHookRan.Load() {
		t.Error("AnyHook OnError hook was NOT fired on rebuilt action")
	}
	if executedHookRan.Load() {
		t.Error("OnExecuted hook MUST NOT fire on failure")
	}
}

func TestBuiltAction_ToBuilder_PanicRecovery(t *testing.T) {
	t.Parallel()

	var panicCaught atomic.Bool

	orig := action.New("panic.node", func(_ context.Context, _ string) (string, error) {
		panic("database connection pool melted")
	}).
		AnyHook(action.AnyHook{
			OnPanic: func(_ context.Context, _ any, recovered any, _ *action.Meta) {
				if str, ok := recovered.(string); ok && strings.Contains(str, "database connection pool melted") {
					panicCaught.Store(true)
				}
			},
		}).
		Build()

	rebuilt := orig.ToBuilder().Build()

	// Bad path: A panic inside the handler must be recovered into an xerr.AppError
	res, err := rebuilt.Do(context.Background(), "req")
	if res != "" {
		t.Fatalf("expected zero value on panic, got %q", res)
	}
	if err == nil {
		t.Fatal("expected panic to be converted into an error, got nil")
	}

	var appErr *xerr.AppError
	if !errors.As(err, &appErr) || appErr.Kind != xerr.KindInternal {
		t.Fatalf("expected KindInternal error on panic, got %v", err)
	}
	if !panicCaught.Load() {
		t.Fatal("OnPanic hook was NOT called on rebuilt action")
	}
}

func TestBuiltAction_ToBuilder_ContextCancellationAndTimeout(t *testing.T) {
	t.Parallel()

	var (
		cancelHookRan atomic.Bool
		errorHookRan  atomic.Bool
	)

	orig := action.New("slow.node", func(ctx context.Context, _ string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return "done", nil
		}
	}).
		Timeout(50 * time.Millisecond).
		Hook(action.Hook[string, string]{
			OnCancel: func(_ context.Context, _ string, _ *action.Meta) {
				cancelHookRan.Store(true)
			},
			OnError: func(_ context.Context, _ string, _ error, _ *action.Meta) {
				errorHookRan.Store(true)
			},
		}).
		Build()

	rebuilt := orig.ToBuilder().Build()

	// 1. Bad path (Timeout): Handler exceeds configured timeout -> DeadlineExceeded triggers OnError
	_, timeoutErr := rebuilt.Do(context.Background(), "req")
	if timeoutErr == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(timeoutErr, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got: %v", timeoutErr)
	}
	if !errorHookRan.Load() {
		t.Error("OnError hook did not fire upon context deadline expiry")
	}

	// 2. Bad path (Cancellation): Context canceled -> context.Canceled triggers OnCancel
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, cancelErr := rebuilt.Do(ctxCanceled, "req")
	if cancelErr == nil {
		t.Fatal("expected context canceled error, got nil")
	}
	if !errors.Is(cancelErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", cancelErr)
	}
	if !cancelHookRan.Load() {
		t.Error("OnCancel hook did not fire upon context cancellation")
	}
}

// ── 3. Security & Auth Guard Enforcement (Happy & Bad Paths) ──────────────────

func TestBuiltAction_ToBuilder_AuthGuardsPreserved(t *testing.T) {
	t.Parallel()

	orig := action.New("secure.delete", func(_ context.Context, id string) (string, error) {
		return "deleted:" + id, nil
	}).
		RequireRole("superadmin").
		RequirePermission("records:delete").
		Build()

	rebuilt := orig.ToBuilder().Build()

	// ❌ BAD PATH 1: No auth context -> MUST be rejected with 403 Forbidden
	_, errNoAuth := rebuilt.Do(context.Background(), "123")
	if errNoAuth == nil {
		t.Fatal("expected 403 error for unauthenticated call, got nil")
	}
	if xerr.KindFrom(errNoAuth) != xerr.KindForbidden {
		t.Fatalf("expected KindForbidden, got kind %s: %v", xerr.KindFrom(errNoAuth), errNoAuth)
	}

	// ❌ BAD PATH 2: Wrong role -> MUST be rejected with 403 Forbidden
	ctxWrongRole := xctx.WithRoles(context.Background(), []string{"guest"})
	_, errWrongRole := rebuilt.Do(ctxWrongRole, "123")
	if errWrongRole == nil {
		t.Fatal("expected 403 error for wrong role, got nil")
	}
	if xerr.KindFrom(errWrongRole) != xerr.KindForbidden {
		t.Fatalf("expected KindForbidden, got: %v", errWrongRole)
	}

	// ❌ BAD PATH 3: Has role but missing required permission
	ctxMissingPerm := xctx.WithRoles(context.Background(), []string{"superadmin"})
	_, errMissingPerm := rebuilt.Do(ctxMissingPerm, "123")
	if errMissingPerm == nil {
		t.Fatal("expected 403 error for missing permission, got nil")
	}

	// ✅ HAPPY PATH: Context satisfies all requirements
	ctxValid := xctx.WithScope(context.Background(), &xctx.RequestScope{})
	ctxValid = xctx.WithRoles(ctxValid, []string{"superadmin"})
	ctxValid = xctx.WithPermissions(ctxValid, []string{"records:delete"})

	res, errValid := rebuilt.Do(ctxValid, "123")
	if errValid != nil {
		t.Fatalf("expected success with valid credentials, got: %v", errValid)
	}
	if res != "deleted:123" {
		t.Fatalf("expected 'deleted:123', got %q", res)
	}
}

// ── 4. Multi-Transport Binding Retention ──────────────────────────────────────

func TestBuiltAction_ToBuilder_MultiTransportRetention(t *testing.T) {
	t.Parallel()

	r1 := httpBinding{Method: "POST", Path: "/api/autonomous/solve"}
	r2 := cliBinding{Command: "autonomous:solve"}

	orig := action.New("multi.transport", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).
		Route(r1, r2).
		Build()

	rebuilt := orig.ToBuilder().Build()

	bindings := rebuilt.GetBindings()
	if len(bindings) != 2 {
		t.Fatalf("expected exactly 2 bindings preserved, got %d", len(bindings))
	}

	b1, ok1 := bindings[0].(httpBinding)
	b2, ok2 := bindings[1].(cliBinding)

	if !ok1 || b1.Method != http.MethodPost || b1.Path != "/api/autonomous/solve" {
		t.Errorf("first binding corrupted: %+v", bindings[0])
	}
	if !ok2 || b2.Command != "autonomous:solve" {
		t.Errorf("second binding corrupted: %+v", bindings[1])
	}
}

// ── 5. Lifecycle Cleanups on Close() ──────────────────────────────────────────

func TestBuiltAction_ToBuilder_CleanupsExecutedAndIdempotent(t *testing.T) {
	t.Parallel()

	actWithCleanup := action.New("direct.clean", func(_ context.Context, _ struct{}) (string, error) {
		return "ok", nil
	}).
		Cache(time.Hour, func(_ struct{}) string { return "key" }).
		Build()

	rebuiltCustom := actWithCleanup.ToBuilder().Build()
	rebuiltCustom.Close()
	rebuiltCustom.Close() // Idempotency check: second Close() must not panic or double-free
}

// ── 6. ApplyDSL Real-World Overlay (Jumalu Regression) ────────────────────────

func TestBuiltAction_ApplyDSL_HappyAndBadPath(t *testing.T) {
	t.Parallel()

	route := httpBinding{Method: "POST", Path: "/api/autonomous/solve"}

	baseAction := action.New("autonomous.solve", func(_ context.Context, goal string) (string, error) {
		if goal == "" {
			return "", xerr.BadRequest("goal required")
		}
		return "solved: " + goal, nil
	}).
		Route(route).
		Build()

	applier, ok := any(baseAction).(action.DSLApplier)
	if !ok {
		t.Fatal("baseAction must implement action.DSLApplier")
	}

	// Apply DSL overlay identical to app.flow
	overlaid := applier.ApplyDSL(action.DSLModifiers{
		SuccessStatus: 200,
		Timeout:       10 * time.Second,
		RateLimit:     50,
		Burst:         100,
		RequiresAuth:  true,
		Roles:         []string{"operator"},
		Tags:          []string{"autonomous", "overlaid"},
	})

	// 1. Assert routes were NOT stripped by ApplyDSL
	bindings := overlaid.GetBindings()
	if len(bindings) != 1 {
		t.Fatalf("CRITICAL BUG: ApplyDSL stripped route bindings! Expected 1, got %d", len(bindings))
	}
	hb, ok := bindings[0].(httpBinding)
	if !ok || hb.Method != http.MethodPost || hb.Path != "/api/autonomous/solve" {
		t.Fatalf("route corrupted: %+v", bindings[0])
	}

	// ❌ BAD PATH 1: No auth context at all -> rejected by RequiresAuth
	_, errNoAuth := overlaid.DoAny(context.Background(), "Build auth service")
	if errNoAuth == nil {
		t.Fatal("expected error after DSL auth overlay without credentials, got nil")
	}

	// ❌ BAD PATH 2: Authenticated (has UserID) but missing required role ("operator") -> rejected by RequireRole
	ctxUserNoRole := xctx.WithUserID(context.Background(), "user-123")
	_, errWrongRole := overlaid.DoAny(ctxUserNoRole, "Build auth service")
	if errWrongRole == nil {
		t.Fatal("expected 403 Forbidden for missing role, got nil")
	}
	if xerr.KindFrom(errWrongRole) != xerr.KindForbidden {
		t.Fatalf("expected Forbidden, got: %v", errWrongRole)
	}

	// ❌ BAD PATH 3: Authenticated + role, but bad input payload -> rejected by business logic
	ctxAuth := xctx.WithUserID(context.Background(), "user-123")
	ctxAuth = xctx.WithRoles(ctxAuth, []string{"operator"})
	_, errBadInput := overlaid.DoAny(ctxAuth, "")
	if errBadInput == nil {
		t.Fatal("expected 400 Bad Request on empty goal, got nil")
	}

	// ✅ HAPPY PATH: Valid auth (UserID) + valid role ("operator") + valid payload
	res, errSuccess := overlaid.DoAny(ctxAuth, "Build auth service")
	if errSuccess != nil {
		t.Fatalf("expected successful execution, got: %v", errSuccess)
	}
	if res != "solved: Build auth service" {
		t.Fatalf("unexpected result: %v", res)
	}
}
