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
