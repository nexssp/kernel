package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestBuilderEventShorthands_SuccessAndError(t *testing.T) {
	t.Parallel()

	var executed, errors int

	ok := action.New("events.ok", func(_ context.Context, value string) (string, error) {
		return value, nil
	}).
		HookSuccessEvent(func() { executed++ }).
		HookErrorEvent(func(error) { errors++ }).
		Build()

	if _, err := ok.Do(t.Context(), "ok"); err != nil {
		t.Fatal(err)
	}

	failed := action.New("events.failed", func(_ context.Context, _ string) (string, error) {
		return "", xerr.Unavailable("temporary")
	}).
		HookSuccessEvent(func() { executed++ }).
		HookErrorEvent(func(err error) {
			if xerr.KindFrom(err) != xerr.KindUnavailable {
				t.Errorf("unexpected error kind: %v", err)
			}
			errors++
		}).
		Build()

	if _, err := failed.Do(t.Context(), "failed"); err == nil {
		t.Fatal("expected error")
	}

	if executed != 1 {
		t.Fatalf("executed events = %d, want 1", executed)
	}
	if errors != 1 {
		t.Fatalf("error events = %d, want 1", errors)
	}
}

func TestBuilderEventShorthands_TimeoutBeforeError(t *testing.T) {
	t.Parallel()

	var events []string
	act := action.New("events.timeout", func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}).
		Timeout(time.Millisecond).
		HookTimeoutEvent(func() { events = append(events, "timeout") }).
		HookErrorEvent(func(error) { events = append(events, "error") }).
		Build()

	if _, err := act.Do(t.Context(), "request"); err == nil {
		t.Fatal("expected timeout error")
	}
	if len(events) != 2 || events[0] != "timeout" || events[1] != "error" {
		t.Fatalf("events = %v, want [timeout error]", events)
	}
}

func TestBuilderEventShorthands_Retry(t *testing.T) {
	t.Parallel()

	var retries int
	attempt := 0

	act := action.New("events.retry", func(_ context.Context, _ string) (string, error) {
		attempt++
		if attempt < 3 {
			return "", xerr.Unavailable("temporary")
		}
		return "ok", nil
	}).
		HookRetryEvent(func(number int, err error) {
			if number != retries+1 {
				t.Errorf("retry number = %d, want %d", number, retries+1)
			}
			if xerr.KindFrom(err) != xerr.KindUnavailable {
				t.Errorf("unexpected retry error: %v", err)
			}
			retries++
		}).
		Retry(3, action.ConstantBackoff(0)).
		Build()

	got, err := act.Do(t.Context(), "request")
	if err != nil || got != "ok" {
		t.Fatalf("result = %q, err = %v", got, err)
	}
	if retries != 2 {
		t.Fatalf("retry events = %d, want 2", retries)
	}
}

func TestBuilderEventShorthands_Cache(t *testing.T) {
	t.Parallel()

	var hits, misses int
	act := action.New("events.cache", func(_ context.Context, value string) (string, error) {
		return value + "-computed", nil
	}).
		HookCacheHitEvent(func() { hits++ }).
		HookCacheMissEvent(func() { misses++ }).
		Cache(time.Minute, func(value string) string { return value }).
		Build()

	first, err := act.Do(t.Context(), "value")
	if err != nil || first != "value-computed" {
		t.Fatalf("first result = %q, err = %v", first, err)
	}
	second, err := act.Do(t.Context(), "value")
	if err != nil || second != "value-computed" {
		t.Fatalf("second result = %q, err = %v", second, err)
	}

	if misses != 1 {
		t.Fatalf("cache misses = %d, want 1", misses)
	}
	if hits != 1 {
		t.Fatalf("cache hits = %d, want 1", hits)
	}
}

func TestBuilderEventShorthands_NilCallbacksAreNoOps(t *testing.T) {
	t.Parallel()

	act := action.New("events.nil", func(_ context.Context, value string) (string, error) {
		return value, nil
	}).
		HookSuccessEvent(nil).
		HookErrorEvent(nil).
		HookTimeoutEvent(nil).
		HookRetryEvent(nil).
		HookCacheHitEvent(nil).
		HookCacheMissEvent(nil).
		HookCoalescedEvent(nil).
		HookDeduplicatedEvent(nil).
		HookCancelEvent(nil).
		Build()

	if got, err := act.Do(t.Context(), "ok"); err != nil || got != "ok" {
		t.Fatalf("result = %q, err = %v", got, err)
	}
}
