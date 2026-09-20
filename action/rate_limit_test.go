package action_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type (
	tenantKey struct{}
	idKey     struct{}
)

func TestRateLimit_Basic(t *testing.T) {
	t.Parallel()

	act := action.New("test.basic.ratelimit", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).RateLimit(1, 2).Build()

	ctx := context.Background()

	if _, err := act.Do(ctx, "a"); err != nil {
		t.Fatalf("1st call failed: %v", err)
	}
	if _, err := act.Do(ctx, "b"); err != nil {
		t.Fatalf("2nd call failed: %v", err)
	}

	_, err := act.Do(ctx, "c")
	if err == nil {
		t.Fatal("expected 3rd call to be rate limited, got nil")
	}
	if xerr.KindFrom(err) != xerr.KindTooManyRequests {
		t.Fatalf("expected KindTooManyRequests, got: %v", err)
	}
}

func TestRateLimitWithKey_IsolatedKeys(t *testing.T) {
	t.Parallel()

	act := action.New("test.keyed.ratelimit", func(_ context.Context, req string) (string, error) {
		return "ok:" + req, nil
	}).RateLimitWithKey(1, 1, func(ctx context.Context) string {
		v, _ := ctx.Value(tenantKey{}).(string)
		return v
	}).Build()

	ctxUser1 := context.WithValue(context.Background(), tenantKey{}, "user1")
	ctxUser2 := context.WithValue(context.Background(), tenantKey{}, "user2")

	if _, err := act.Do(ctxUser1, "req1"); err != nil {
		t.Fatalf("user1 call 1 failed: %v", err)
	}
	if _, err := act.Do(ctxUser1, "req2"); err == nil {
		t.Fatal("expected user1 call 2 to be rate limited")
	}

	if _, err := act.Do(ctxUser2, "req1"); err != nil {
		t.Fatalf("user2 call 1 was unexpectedly blocked: %v", err)
	}
}

func TestMemoryRateLimiter_LRU_BoundedCapacity(t *testing.T) {
	t.Parallel()

	limiter := action.NewMemoryRateLimiterForTest(10, 10, time.Hour, 3)
	ctx := context.Background()

	_, _ = limiter.Allow(ctx, "k1")
	_, _ = limiter.Allow(ctx, "k2")
	_, _ = limiter.Allow(ctx, "k3")

	_, _ = limiter.Allow(ctx, "k1")
	_, _ = limiter.Allow(ctx, "k4")

	if limiter.Len() != 3 {
		t.Fatalf("expected limiter size to remain at 3, got %d", limiter.Len())
	}
	if limiter.Has("k2") {
		t.Fatal("expected k2 (oldest LRU item) to be evicted, but it is still present")
	}
	if !limiter.Has("k1") || !limiter.Has("k3") || !limiter.Has("k4") {
		t.Fatal("expected k1, k3, and k4 to remain in limiter")
	}
}

func TestMemoryRateLimiter_NoGoroutineLeak(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	for i := range 100 {
		_ = action.New(fmt.Sprintf("act.%d", i), func(_ context.Context, _ struct{}) (struct{}, error) {
			return struct{}{}, nil
		}).RateLimitWithKey(10, 2, func(_ context.Context) string {
			return "k"
		}).Build()
	}

	currentGoroutines := runtime.NumGoroutine()
	if currentGoroutines > initialGoroutines+2 {
		t.Fatalf("goroutine leak detected: initial=%d, current=%d", initialGoroutines, currentGoroutines)
	}
}

func TestMemoryRateLimiter_ConcurrentAccessRace(t *testing.T) {
	t.Parallel()

	act := action.New("concurrent.ratelimit", func(_ context.Context, _ int) (int, error) {
		return 1, nil
	}).RateLimitWithKey(1000, 100, func(ctx context.Context) string {
		v, _ := ctx.Value(idKey{}).(int)
		return fmt.Sprintf("key_%d", v%5)
	}).Build()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.WithValue(context.Background(), idKey{}, id)
			_, _ = act.Do(ctx, id)
		}(i)
	}
	wg.Wait()
}

func TestRateLimit_ContextCanceled(t *testing.T) {
	t.Parallel()

	act := action.New("canceled.test", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).RateLimit(1, 1).Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := act.Do(ctx, "req")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}
