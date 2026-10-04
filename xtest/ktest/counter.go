package ktest

import (
	"sync/atomic"
	"testing"
)

// Counter records action executions without requiring a custom atomic in each test.
type Counter struct {
	calls atomic.Int64
}

// NewCounter returns an empty execution counter.
func NewCounter() *Counter { return new(Counter) }

// Inc records one execution.
func (c *Counter) Inc() { c.calls.Add(1) }

// Load returns the number of recorded executions.
func (c *Counter) Load() int64 { return c.calls.Load() }

// Require fails unless the counter equals want.
func (c *Counter) Require(tb testing.TB, want int64) {
	tb.Helper()
	if got := c.Load(); got != want {
		tb.Fatalf("execution count = %d, want %d", got, want)
	}
}

// Reset returns the counter to zero.
func (c *Counter) Reset() { c.calls.Store(0) }

// RequireAtLeast fails unless the counter is >= want.
func (c *Counter) RequireAtLeast(tb testing.TB, want int64) {
	tb.Helper()
	if got := c.Load(); got < want {
		tb.Fatalf("execution count = %d, want >= %d", got, want)
	}
}

// RequireAtMost fails unless the counter is <= want. Common in cache and
// coalesce tests where the assertion is an upper bound, not exact.
func (c *Counter) RequireAtMost(tb testing.TB, want int64) {
	tb.Helper()
	if got := c.Load(); got > want {
		tb.Fatalf("execution count = %d, want <= %d", got, want)
	}
}
