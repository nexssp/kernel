package action_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

type dslReq struct {
	Val string `json:"val"`
}

func TestWithDSL_AppliesAllFields(t *testing.T) {
	t.Parallel()

	type payload struct{}

	act := action.New[dslReq, string]("svc.op", func(_ context.Context, r dslReq) (string, error) {
		return r.Val, nil
	}).
		WithDSL(action.DSLModifiers{
			Scope:         "internal",
			RequiresAuth:  true,
			Roles:         []string{"admin", "operator"},
			Permissions:   []string{"users:write"},
			Features:      []string{"beta"},
			RateLimit:     100,
			Burst:         200,
			Concurrency:   10,
			Timeout:       5 * time.Second,
			RetryMax:      3,
			Idempotent:    true,
			CacheTTL:      30 * time.Second,
			Tags:          []string{"users", "write"},
			SuccessStatus: 201,
		}).
		Build()

	meta := act.Describe()
	if meta.Scope != action.ScopeInternal {
		t.Errorf("Scope = %q", meta.Scope)
	}
	if !meta.RequiresAuth {
		t.Errorf("RequiresAuth = false")
	}
	if len(meta.RequiredRoles) != 2 || meta.RequiredRoles[0] != "admin" {
		t.Errorf("Roles = %v", meta.RequiredRoles)
	}
	if len(meta.RequiredPermissions) != 1 || meta.RequiredPermissions[0] != "users:write" {
		t.Errorf("Permissions = %v", meta.RequiredPermissions)
	}
	if len(meta.RequiredFeatures) != 1 || meta.RequiredFeatures[0] != "beta" {
		t.Errorf("Features = %v", meta.RequiredFeatures)
	}
	if meta.SuccessStatus != 201 {
		t.Errorf("SuccessStatus = %d", meta.SuccessStatus)
	}
	if meta.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v", meta.Timeout)
	}
	if meta.ConcurrencyLimit != 10 {
		t.Errorf("ConcurrencyLimit = %d", meta.ConcurrencyLimit)
	}
	if !meta.Idempotency.Enabled {
		t.Errorf("Idempotent.Enabled = false")
	}
	if meta.CacheTTL != 30*time.Second {
		t.Errorf("CacheTTL = %v", meta.CacheTTL)
	}
	if meta.RetryMax != 3 {
		t.Errorf("RetryMax = %d", meta.RetryMax)
	}
	if len(meta.Tags) != 2 || meta.Tags[0] != "users" || meta.Tags[1] != "write" {
		t.Errorf("Tags = %v", meta.Tags)
	}

	_ = payload{} // unused, kept for readability
}

func TestWithDSL_ZeroValueIsNoop(t *testing.T) {
	t.Parallel()

	act := action.New[dslReq, string]("svc.noop", func(_ context.Context, r dslReq) (string, error) {
		return r.Val, nil
	}).
		Description("original").
		WithDSL(action.DSLModifiers{}).
		Build()

	meta := act.Describe()
	if meta.Description != "original" {
		t.Errorf("Description changed: %q", meta.Description)
	}
	if meta.Scope != action.ScopePublic {
		t.Errorf("Scope default changed: %q", meta.Scope)
	}
	if meta.SuccessStatus != 0 {
		t.Errorf("SuccessStatus changed: %d", meta.SuccessStatus)
	}
}

func TestApplyDSL_NonGenericBridge(t *testing.T) {
	t.Parallel()

	var actions []action.AnyAction
	actions = append(actions, action.New[dslReq, string]("svc.bridge", func(_ context.Context, r dslReq) (string, error) {
		return r.Val, nil
	}).Build())

	for i, act := range actions {
		applier, ok := act.(action.DSLApplier)
		if !ok {
			t.Fatalf("action %d does not implement DSLApplier", i)
		}
		actions[i] = applier.ApplyDSL(action.DSLModifiers{
			RequiresAuth:  true,
			Roles:         []string{"admin"},
			SuccessStatus: 202,
		})
	}

	meta := actions[0].Describe()
	if !meta.RequiresAuth {
		t.Errorf("RequiresAuth = false after ApplyDSL")
	}
	if len(meta.RequiredRoles) != 1 || meta.RequiredRoles[0] != "admin" {
		t.Errorf("Roles = %v", meta.RequiredRoles)
	}
	if meta.SuccessStatus != 202 {
		t.Errorf("SuccessStatus = %d", meta.SuccessStatus)
	}
}

func TestWithDSL_CustomBackoff(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32

	act := action.New[dslReq, string]("svc.backoff", func(_ context.Context, r dslReq) (string, error) {
		if attempts.Add(1) < 3 {
			return "", errors.New("transient")
		}
		return r.Val, nil
	}).
		WithDSL(action.DSLModifiers{
			RetryMax:     5,
			RetryBackoff: action.ConstantBackoff(time.Millisecond),
			RetryIf:      action.AlwaysRetryPredicate,
		}).
		Build()

	got, err := act.Do(context.Background(), dslReq{Val: "ok"})
	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if got != "ok" {
		t.Errorf("got %q", got)
	}
	if n := attempts.Load(); n != 3 {
		t.Errorf("attempts = %d, want 3", n)
	}
}

func TestWithDSL_Hooks(t *testing.T) {
	t.Parallel()

	var called atomic.Bool

	hook := action.AnyHook{
		Before: func(ctx context.Context, req any, meta *action.Meta) (context.Context, error) {
			called.Store(true)
			return ctx, nil
		},
	}

	act := action.New[dslReq, string]("svc.hook", func(_ context.Context, r dslReq) (string, error) {
		return r.Val, nil
	}).
		WithDSL(action.DSLModifiers{Hooks: []action.AnyHook{hook}}).
		Build()

	if _, err := act.Do(context.Background(), dslReq{Val: "x"}); err != nil {
		t.Fatal(err)
	}
	if !called.Load() {
		t.Errorf("hook was not called")
	}
}

func TestWithDSL_CacheKeyFnRaw(t *testing.T) {
	t.Parallel()

	calls := atomic.Int32{}

	keyFn := func(r dslReq) string { return r.Val }

	act := action.New[dslReq, string]("svc.cachekey", func(_ context.Context, r dslReq) (string, error) {
		calls.Add(1)
		return r.Val, nil
	}).
		WithDSL(action.DSLModifiers{
			CacheTTL:      time.Minute,
			CacheKeyFnRaw: keyFn,
		}).
		Build()

	ctx := context.Background()
	if _, err := act.Do(ctx, dslReq{Val: "same"}); err != nil {
		t.Fatal(err)
	}
	if _, err := act.Do(ctx, dslReq{Val: "same"}); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("expected cache hit — calls = %d, want 1", n)
	}
}
