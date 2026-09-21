package xtest

import (
	"runtime"
	"testing"
	"time"
)

// RequireNoGoroutineLeak waits until the goroutine count returns to baseline.
func RequireNoGoroutineLeak(tb testing.TB, baseline int, timeout time.Duration) {
	tb.Helper()
	RequireGoroutinesAtMost(tb, baseline, timeout, 0)
}

// RequireGoroutinesAtMost waits for goroutines to settle at or below
// baseline+allowedDelta. The allowance is useful because the testing runtime
// and unrelated parallel tests may transiently create goroutines.
func RequireGoroutinesAtMost(tb testing.TB, baseline int, timeout time.Duration, allowedDelta int) {
	tb.Helper()
	if baseline < 0 || allowedDelta < 0 || timeout <= 0 {
		tb.Fatalf("xtest.RequireGoroutinesAtMost: invalid baseline=%d timeout=%s allowedDelta=%d", baseline, timeout, allowedDelta)
	}

	deadline := time.Now().Add(timeout)
	limit := baseline + allowedDelta
	current := runtime.NumGoroutine()
	for current > limit && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		current = runtime.NumGoroutine()
	}
	if current > limit {
		tb.Fatalf("goroutine leak: baseline=%d allowed_delta=%d current=%d", baseline, allowedDelta, current)
	}
}
