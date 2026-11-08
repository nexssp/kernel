package action_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

func TestIdempotencyStorageKeyIsScopedByActionPrincipalAndAuthorization(t *testing.T) {
	store := action.NewMemoryIdempotencyStore(time.Hour)
	var firstCalls, secondCalls atomic.Int32
	first := action.New("scope.action.first", func(_ context.Context, request string) (string, error) {
		return "first:" + request + ":" + strconv.Itoa(int(firstCalls.Add(1))), nil
	}).IdempotentWithConfig(action.IdempotencyConfig{Store: store}).Build()
	second := action.New("scope.action.second", func(_ context.Context, request string) (string, error) {
		return "second:" + request + ":" + strconv.Itoa(int(secondCalls.Add(1))), nil
	}).IdempotentWithConfig(action.IdempotencyConfig{Store: store}).Build()

	principal := func(user, tenant, permission string) context.Context {
		ctx := xctx.WithIdempotencyKey(context.Background(), "client-key-1")
		ctx = xctx.WithUserID(ctx, user)
		ctx = xctx.WithTenantID(ctx, tenant)
		if permission != "" {
			ctx = xctx.WithPermissions(ctx, []string{permission})
		}
		return ctx
	}

	ctx := principal("user-a", "tenant-a", "invoice:read")
	for range 2 {
		got, err := first.Do(ctx, "payload")
		if err != nil || got != "first:payload:1" {
			t.Fatalf("same-principal replay = (%q, %v), want cached first:payload:1", got, err)
		}
	}
	if got, err := second.Do(ctx, "payload"); err != nil || got != "second:payload:1" {
		t.Fatalf("different action result = (%q, %v), want second:payload:1", got, err)
	}
	for i, scope := range []context.Context{
		principal("user-a", "tenant-b", "invoice:read"),
		principal("user-b", "tenant-a", "invoice:read"),
		principal("user-a", "tenant-a", "invoice:write"),
	} {
		want := "first:payload:" + string(rune('2'+i))
		if got, err := first.Do(scope, "payload"); err != nil || got != want {
			t.Fatalf("scope %d result = (%q, %v), want independent result %q", i, got, err, want)
		}
	}
	if firstCalls.Load() != 4 || secondCalls.Load() != 1 {
		t.Fatalf("handler executions = (%d, %d), want (4, 1)", firstCalls.Load(), secondCalls.Load())
	}
}

func TestIdempotencyRequestKeyFuncForTypedRequest(t *testing.T) {
	type request struct {
		Key   string
		Value string
	}
	store := action.NewMemoryIdempotencyStore(time.Hour)
	act := action.New("scope.typed-key", func(_ context.Context, req request) (string, error) {
		return req.Value, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Store: store,
		RequestKeyFunc: func(value any) (string, error) {
			req, ok := value.(request)
			if !ok {
				return "", errors.New("unexpected request type")
			}
			return req.Key, nil
		},
	}).Build()

	if got, err := act.Do(context.Background(), request{Key: "typed-key", Value: "first"}); err != nil || got != "first" {
		t.Fatalf("first typed-key result = (%q, %v)", got, err)
	}
	_, err := act.Do(context.Background(), request{Key: "typed-key", Value: "different"})
	if err == nil || xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("typed-key payload conflict = %v, want Conflict", err)
	}
}

func TestIdempotencyDirectMiddlewareRequiresStableNamespace(t *testing.T) {
	var calls atomic.Int32
	middleware := action.IdempotencyMiddleware[int, int](nil, action.IdempotencyConfig{Enabled: true})
	handler := middleware(func(context.Context, int) (int, error) {
		calls.Add(1)
		return 1, nil
	})
	ctx := xctx.WithIdempotencyKey(context.Background(), "explicit-key")
	_, err := handler(ctx, 1)
	if err == nil || xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("missing namespace error = %v, want Validation", err)
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran without a stable idempotency namespace")
	}
}

func TestIdempotencyRejectsInvalidExplicitKey(t *testing.T) {
	var calls atomic.Int32
	act := action.New("scope.invalid-key", func(context.Context, int) (int, error) {
		calls.Add(1)
		return 1, nil
	}).Idempotent().Build()
	ctx := xctx.WithIdempotencyKey(context.Background(), "bad key\n")
	_, err := act.Do(ctx, 1)
	if err == nil || xerr.KindFrom(err) != xerr.KindBadRequest {
		t.Fatalf("invalid key error = %v, want BadRequest", err)
	}
	if calls.Load() != 0 {
		t.Fatal("handler ran with an invalid idempotency key")
	}
}
