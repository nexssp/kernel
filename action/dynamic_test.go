// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
package action_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type dummyHTTPRoute struct {
	Method string
	Path   string
}

type dummyStringer struct {
	val string
}

func (d dummyStringer) String() string {
	return d.val
}

type UserDTO struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type ResultDTO struct {
	Message string `json:"message"`
	Success bool   `json:"success"`
}

// ── 1. Contract & Binding Preservation (Regression Test) ──────────────────────

func TestDynamic_PreservesContractBindingsAndHooks(t *testing.T) {
	t.Parallel()

	var hookFired atomic.Bool
	route := dummyHTTPRoute{Method: "POST", Path: "/api/autonomous/solve"}

	// Concrete base action with bindings, auth, tags, and lifecycle hooks
	base := action.New("orders.process", func(_ context.Context, req UserDTO) (ResultDTO, error) {
		return ResultDTO{Message: "processed: " + req.Name, Success: true}, nil
	}).
		Description("Core order processing action").
		Tag("orders", "production").
		Route(route).
		Timeout(2 * time.Second).
		RequireRole("operator").
		AnyHook(action.AnyHook{
			Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
				hookFired.Store(true)
				return ctx, nil
			},
		}).
		Build()

	// Wrap dynamically
	dynBuilder := action.Dynamic(base)
	dynAct := dynBuilder.Build()

	// 1. Invariant: Bindings MUST NOT be stripped
	bindings := dynAct.GetBindings()
	if len(bindings) != 1 {
		t.Fatalf("expected 1 binding preserved, got %d", len(bindings))
	}
	r, ok := bindings[0].(dummyHTTPRoute)
	if !ok || r.Method != "POST" || r.Path != "/api/autonomous/solve" {
		t.Fatalf("binding corrupted or modified: %+v", bindings[0])
	}

	// 2. Invariant: Operational metadata must match the wrapped target
	meta := dynAct.Describe()
	if meta.Name != "orders.process" {
		t.Errorf("name = %q, want orders.process", meta.Name)
	}
	if meta.Description != "Core order processing action" {
		t.Errorf("description = %q", meta.Description)
	}
	if meta.Timeout != 2*time.Second {
		t.Errorf("timeout = %v, want 2s", meta.Timeout)
	}
	if !reflect.DeepEqual(meta.Tags, []string{"orders", "production"}) {
		t.Errorf("tags = %v, want [orders production]", meta.Tags)
	}
	if len(meta.RequiredRoles) != 1 || meta.RequiredRoles[0] != "operator" {
		t.Errorf("required roles = %v, want [operator]", meta.RequiredRoles)
	}

	// 3. Invariant: Inherited hooks must be preserved and executed
	if len(dynAct.GetAnyHooks()) != 1 {
		t.Fatalf("expected 1 AnyHook preserved, got %d", len(dynAct.GetAnyHooks()))
	}
}

func TestDynamic_ChainingAdditionalBindingsAndHooks(t *testing.T) {
	t.Parallel()

	r1 := dummyHTTPRoute{Method: "GET", Path: "/api/v1/source"}
	r2 := dummyHTTPRoute{Method: "POST", Path: "/api/v2/override"}

	base := action.New("source.read", func(_ context.Context, in string) (string, error) {
		return "ok:" + in, nil
	}).
		Route(r1).
		Build()

	// Add new bindings & hooks on the dynamic wrapper
	dyn := action.Dynamic(base).
		Route(r2).
		Tag("wrapper").
		Build()

	bindings := dyn.GetBindings()
	if len(bindings) != 2 {
		t.Fatalf("expected 2 bindings on chained dynamic action, got %d", len(bindings))
	}

	meta := dyn.Describe()
	if !reflect.DeepEqual(meta.Tags, []string{"wrapper"}) {
		t.Errorf("tags = %v, want [wrapper]", meta.Tags)
	}
}

// ── 2. Defensive Edge Cases & Nil Safety ──────────────────────────────────────

func TestDynamic_NilActionPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected Dynamic(nil) to panic defensively, got nil")
		}
	}()

	_ = action.Dynamic(nil)
}

func TestDynamic_NilPayloadExecution(t *testing.T) {
	t.Parallel()

	act := action.New("param.none", func(_ context.Context, _ struct{}) (string, error) {
		return "success", nil
	}).Build()

	dyn := action.Dynamic(act).Build()

	res, err := dyn.DoAny(context.Background(), nil)
	if err != nil {
		t.Fatalf("expected nil input to map to struct{}, got err: %v", err)
	}
	if res != "success" {
		t.Fatalf("expected 'success', got %v", res)
	}
}

// ── 3. Execution Bad Paths & Error Fidelity ───────────────────────────────────

func TestDynamic_ErrorPropagationAndUnwrapping(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("upstream gateway connection reset")
	appValidationErr := xerr.Validation("invalid payload structure")

	failingBase := action.New("fail.node", func(_ context.Context, mode string) (string, error) {
		if mode == "sentinel" {
			return "", sentinel
		}
		if mode == "app_error" {
			return "", appValidationErr
		}
		return "ok", nil
	}).Build()

	dyn := action.Dynamic(failingBase).Build()
	ctx := context.Background()

	// 1. Standard Go sentinel error preservation
	_, err1 := dyn.DoAny(ctx, "sentinel")
	if !errors.Is(err1, sentinel) {
		t.Fatalf("expected error to unwrap to sentinel, got: %v", err1)
	}

	// 2. Structured AppError taxonomy preservation
	_, err2 := dyn.DoAny(ctx, "app_error")
	if !errors.Is(err2, appValidationErr) {
		t.Fatalf("expected error to unwrap to AppError, got: %v", err2)
	}
	if xerr.KindFrom(err2) != xerr.KindValidation {
		t.Fatalf("expected KindValidation, got: %s", xerr.KindFrom(err2))
	}
}

func TestDynamic_PanicRecoveryIntoInternalError(t *testing.T) {
	t.Parallel()

	panickingBase := action.New("panic.node", func(_ context.Context, _ string) (string, error) {
		panic("nil pointer dereference inside domain service")
	}).Build()

	dyn := action.Dynamic(panickingBase).Build()

	res, err := dyn.DoAny(context.Background(), "trigger")
	if res != "" {
		t.Fatalf("expected empty string zero-value on panic, got %q", res)
	}
	if err == nil {
		t.Fatal("expected panic to be recovered into an error, got nil")
	}

	if xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal error on recovered panic, got: %v", err)
	}
	if !strings.Contains(err.Error(), "nil pointer dereference") {
		t.Fatalf("panic message lost in converted error: %v", err)
	}
}

func TestDynamic_ContextCancellationAndDeadline(t *testing.T) {
	t.Parallel()

	slowBase := action.New("slow.node", func(ctx context.Context, _ string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return "done", nil
		}
	}).Build()

	dyn := action.Dynamic(slowBase).Build()

	// 1. Timeout path
	timeoutCtx, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelTimeout()

	_, errTimeout := dyn.DoAny(timeoutCtx, "req")
	if !errors.Is(errTimeout, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got: %v", errTimeout)
	}

	// 2. Explicit cancellation path
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc() // Cancel immediately

	_, errCancel := dyn.DoAny(cancelCtx, "req")
	if !errors.Is(errCancel, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", errCancel)
	}
}

// ── 4. Coerce[T] Comprehensive Edge Cases (Happy & Bad Paths) ─────────────────

func TestCoerce_HappyPaths(t *testing.T) {
	t.Parallel()

	// Direct matching type
	vInt, err := action.Coerce[int](42)
	if err != nil || vInt != 42 {
		t.Fatalf("failed direct int coerce: %v", err)
	}

	// String conversions: fmt.Stringer
	s1, err := action.Coerce[string](dummyStringer{val: "hello-stringer"})
	if err != nil || s1 != "hello-stringer" {
		t.Fatalf("failed Stringer coerce: %v", err)
	}

	// String conversions: []byte
	s2, err := action.Coerce[string]([]byte("raw-bytes"))
	if err != nil || s2 != "raw-bytes" {
		t.Fatalf("failed []byte -> string coerce: %v", err)
	}

	// Byte slice conversions: string
	b1, err := action.Coerce[[]byte]("text-to-bytes")
	if err != nil || string(b1) != "text-to-bytes" {
		t.Fatalf("failed string -> []byte coerce: %v", err)
	}

	// Map text-key extraction (common in agent prompts)
	mapPayload := map[string]any{"content": "extracted text payload"}
	extracted, err := action.Coerce[string](mapPayload)
	if err != nil || extracted != "extracted text payload" {
		t.Fatalf("failed map content extraction: %v", err)
	}

	// Structural JSON bridge between distinct struct types
	inputMap := map[string]any{"name": "Alice", "age": 28}
	user, err := action.Coerce[UserDTO](inputMap)
	if err != nil || user.Name != "Alice" || user.Age != 28 {
		t.Fatalf("failed map -> struct coerce: %v", err)
	}
}

func TestCoerce_BadPaths(t *testing.T) {
	t.Parallel()

	// Incompatible scalar types that cannot convert via JSON
	_, errFunc := action.Coerce[string](func() {})
	if errFunc == nil {
		t.Fatal("expected error coercing un-marshalable func(), got nil")
	}

	// JSON unmarshal type mismatch: string into int
	_, errMismatch := action.Coerce[int]("not-a-number")
	if errMismatch == nil {
		t.Fatal("expected error coercing non-numeric string into int, got nil")
	}

	// Struct schema violation: wrong primitive types
	invalidMap := map[string]any{"name": "Bob", "age": "twenty-five"}
	_, errStruct := action.Coerce[UserDTO](invalidMap)
	if errStruct == nil {
		t.Fatal("expected unmarshal error for invalid struct fields, got nil")
	}
}
