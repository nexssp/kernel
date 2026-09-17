package action_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestMemoryRateLimiter_GoroutineLeak(t *testing.T) {
	time.Sleep(100 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	for range 50 {
		_ = action.New("leak.action", func(_ context.Context, _ struct{}) (string, error) {
			return "ok", nil
		}).RateLimitWithKey(10, 2, func(_ context.Context) string {
			return "test-key"
		}).Build()
	}

	time.Sleep(100 * time.Millisecond)
	currentGoroutines := runtime.NumGoroutine()

	if currentGoroutines > initialGoroutines+5 {
		t.Fatalf("goroutine leak: expected around %d goroutines, got %d", initialGoroutines, currentGoroutines)
	}
}
