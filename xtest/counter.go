package xtest

import (
	"sync/atomic"
	"testing"
	"time"
)

func WaitForAtLeast(tb testing.TB, counter *atomic.Int32, minimum int32, timeout time.Duration) {
	tb.Helper()
	Eventually(tb, timeout, func() bool {
		return counter.Load() >= minimum
	})
}

func WaitForAtMost(tb testing.TB, counter *atomic.Int32, maximum int32, timeout time.Duration) {
	tb.Helper()
	Eventually(tb, timeout, func() bool {
		return counter.Load() <= maximum
	})
}
