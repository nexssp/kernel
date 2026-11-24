package action_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

func TestBuilder_Idempotent(t *testing.T) {
	t.Parallel()
	act := action.New("idem.test", func(_ context.Context, req string) (string, error) {
		return req, nil
	}).Idempotent().Build()

	meta := act.GetMeta()
	if !meta.Idempotency.Enabled {
		t.Fatal("expected idempotency to be enabled")
	}
	if meta.Idempotency.Header() != "Idempotency-Key" {
		t.Fatal("expected default header")
	}
}

func TestBuilder_IdempotentWithConfig(t *testing.T) {
	t.Parallel()
	act := action.New("idem.custom", func(_ context.Context, req string) (string, error) {
		return req, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		TTL:       10 * time.Second,
		KeyHeader: "X-Key",
	}).Build()

	meta := act.GetMeta()
	if !meta.Idempotency.Enabled {
		t.Fatal("expected idempotency enabled")
	}
	if meta.Idempotency.Header() != "X-Key" {
		t.Fatalf("expected header X-Key, got %s", meta.Idempotency.Header())
	}
	if meta.Idempotency.Namespace != "idem.custom" {
		t.Fatalf("expected final action namespace idem.custom, got %q", meta.Idempotency.Namespace)
	}
	//nolint:musttag // This test verifies encoding/json's current metadata shape.
	encoded, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal action metadata: %v", err)
	}
	if strings.Contains(string(encoded), "Store") || strings.Contains(string(encoded), "KeyFunc") {
		t.Fatalf("runtime idempotency configuration leaked into metadata: %s", encoded)
	}
}

func TestBuilder_IdempotencyNamespaceUsesFinalName(t *testing.T) {
	store := action.NewMemoryIdempotencyStore(time.Hour)
	builder := action.New("before.rename", func(_ context.Context, req string) (string, error) {
		return req, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{Store: store})
	builder.Name("after.rename")
	act := builder.Build()

	if got := act.GetMeta().Idempotency.Namespace; got != "after.rename" {
		t.Fatalf("idempotency namespace = %q, want after.rename", got)
	}
}

func TestIdempotencyRunsBeforeCache(t *testing.T) {
	cacheStore := newMockStore()
	if err := cacheStore.Set(context.Background(), "shared-cache-key", "cached response", time.Minute); err != nil {
		t.Fatalf("prepopulate cache: %v", err)
	}
	idempotencyStore := action.NewMemoryIdempotencyStore(time.Hour)
	var handlerCalls, cacheHits int
	act := action.New("idem.cache.order", func(_ context.Context, req string) (string, error) {
		handlerCalls++
		return "response:" + req, nil
	}).
		Cache(time.Minute, func(string) string { return "shared-cache-key" }, cacheStore).
		HookCacheHit(func(context.Context, string, string, *action.Meta) { cacheHits++ }).
		IdempotentWithConfig(action.IdempotencyConfig{Store: idempotencyStore}).
		Build()

	ctx := xctx.WithIdempotencyKey(context.Background(), "cache-order-key")
	got, err := act.Do(ctx, "first payload")
	if err != nil || got != "cached response" {
		t.Fatalf("first call = %q / %v, want cached response / nil", got, err)
	}
	if handlerCalls != 0 || cacheHits != 1 {
		t.Fatalf("first call handler/cache-hit counts = %d/%d, want 0/1", handlerCalls, cacheHits)
	}

	_, err = act.Do(ctx, "different payload")
	if xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("same key with different payload error = %v, want Conflict", err)
	}
	if handlerCalls != 0 || cacheHits != 1 {
		t.Fatalf("conflicting call reached cache/handler: counts = %d/%d, want 0/1", handlerCalls, cacheHits)
	}
}

func TestIdempotentReplayRunsRateLimit(t *testing.T) {
	var handlerCalls int
	// A zero refill rate makes the two-token burst deterministic.
	act := action.New("idem.rate-limit.replay", func(_ context.Context, _ struct{}) (string, error) {
		handlerCalls++
		return "ok", nil
	}).RateLimit(0, 2).IdempotentWithConfig(action.IdempotencyConfig{
		Store: action.NewMemoryIdempotencyStore(time.Hour),
	}).Build()
	ctx := xctx.WithIdempotencyKey(context.Background(), "rate-limit-replay-key")

	if got, err := act.Do(ctx, struct{}{}); err != nil || got != "ok" {
		t.Fatalf("first call = %q / %v, want ok / nil", got, err)
	}
	got, err := act.Do(ctx, struct{}{})
	if err != nil || got != "ok" {
		t.Fatalf("allowed replay = %q / %v, want cached ok / nil", got, err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls after allowed replay = %d, want 1", handlerCalls)
	}

	_, err = act.Do(ctx, struct{}{})
	if xerr.KindFrom(err) != xerr.KindTooManyRequests {
		t.Fatalf("rate-limited replay error = %v, want TooManyRequests", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}
}

func TestIdempotentReplayRunsCustomAuthorizationMiddleware(t *testing.T) {
	allowed := true
	var handlerCalls int
	act := action.New("idem.custom-auth.replay", func(_ context.Context, _ struct{}) (string, error) {
		handlerCalls++
		return "ok", nil
	}).Use(func(next action.Fn[struct{}, string]) action.Fn[struct{}, string] {
		return func(ctx context.Context, req struct{}) (string, error) {
			if !allowed {
				return "", xerr.Forbidden("authorization revoked")
			}
			return next(ctx, req)
		}
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Store: action.NewMemoryIdempotencyStore(time.Hour),
	}).Build()
	ctx := xctx.WithIdempotencyKey(context.Background(), "custom-auth-replay-key")

	if got, err := act.Do(ctx, struct{}{}); err != nil || got != "ok" {
		t.Fatalf("first call = %q / %v, want ok / nil", got, err)
	}
	got, err := act.Do(ctx, struct{}{})
	if err != nil || got != "ok" {
		t.Fatalf("allowed replay = %q / %v, want cached ok / nil", got, err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls after allowed replay = %d, want 1", handlerCalls)
	}

	allowed = false
	_, err = act.Do(ctx, struct{}{})
	if xerr.KindFrom(err) != xerr.KindForbidden {
		t.Fatalf("revoked replay error = %v, want Forbidden", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}
}

func TestIdempotentReplayRunsAuthorizationRegisteredAfterIdempotency(t *testing.T) {
	allowed := true
	var middlewareCalls, handlerCalls int
	act := action.New("idem.custom-auth.after-idempotency", func(_ context.Context, _ struct{}) (string, error) {
		handlerCalls++
		return "ok", nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Store: action.NewMemoryIdempotencyStore(time.Hour),
	}).Use(func(next action.Fn[struct{}, string]) action.Fn[struct{}, string] {
		return func(ctx context.Context, req struct{}) (string, error) {
			middlewareCalls++
			if !allowed {
				return "", xerr.Forbidden("authorization revoked")
			}
			return next(ctx, req)
		}
	}).Build()
	ctx := xctx.WithIdempotencyKey(context.Background(), "custom-auth-after-idempotency-key")

	for range 2 {
		if got, err := act.Do(ctx, struct{}{}); err != nil || got != "ok" {
			t.Fatalf("allowed call = %q / %v, want ok / nil", got, err)
		}
	}
	if middlewareCalls != 2 || handlerCalls != 1 {
		t.Fatalf("middleware/handler calls after replay = %d/%d, want 2/1", middlewareCalls, handlerCalls)
	}

	allowed = false
	_, err := act.Do(ctx, struct{}{})
	if xerr.KindFrom(err) != xerr.KindForbidden {
		t.Fatalf("revoked replay error = %v, want Forbidden", err)
	}
	if middlewareCalls != 3 || handlerCalls != 1 {
		t.Fatalf("middleware/handler calls after denied replay = %d/%d, want 3/1", middlewareCalls, handlerCalls)
	}
}

func TestUseOnExecutionSkipsSuccessfulIdempotencyReplay(t *testing.T) {
	var middlewareCalls, handlerCalls int
	act := action.New("idem.execution-only.replay", func(_ context.Context, _ struct{}) (string, error) {
		handlerCalls++
		return "ok", nil
	}).UseOnExecution(func(next action.Fn[struct{}, string]) action.Fn[struct{}, string] {
		return func(ctx context.Context, req struct{}) (string, error) {
			middlewareCalls++
			return next(ctx, req)
		}
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Store: action.NewMemoryIdempotencyStore(time.Hour),
	}).Build()
	ctx := xctx.WithIdempotencyKey(context.Background(), "execution-only-replay-key")

	for range 2 {
		if got, err := act.Do(ctx, struct{}{}); err != nil || got != "ok" {
			t.Fatalf("call = %q / %v, want ok / nil", got, err)
		}
	}
	if middlewareCalls != 1 || handlerCalls != 1 {
		t.Fatalf("execution middleware/handler calls = %d/%d, want 1/1", middlewareCalls, handlerCalls)
	}
}
