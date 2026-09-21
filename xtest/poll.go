package xtest

import (
	"testing"
	"time"
)

const DefaultPollInterval = 2 * time.Millisecond

func Eventually(tb testing.TB, timeout time.Duration, cond func() bool) {
	tb.Helper()
	EventuallyEvery(tb, timeout, DefaultPollInterval, cond)
}

func EventuallyEvery(tb testing.TB, timeout, interval time.Duration, cond func() bool) {
	tb.Helper()
	if cond == nil {
		tb.Fatal("xtest.EventuallyEvery: nil condition")
	}
	if cond() {
		return
	}
	if timeout <= 0 {
		tb.Fatalf("xtest.EventuallyEvery: condition not met (timeout=%s)", timeout)
	}

	interval = normalizeInterval(interval)
	deadline := time.Now().Add(timeout)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			tb.Fatalf("xtest.EventuallyEvery: condition not met within %s", timeout)
		}
		time.Sleep(min(interval, remaining))
		if cond() {
			return
		}
	}
}

func EventuallyE(tb testing.TB, timeout time.Duration, fn func() error) error {
	tb.Helper()
	return EventuallyEveryE(tb, timeout, DefaultPollInterval, fn)
}

func EventuallyEveryE(tb testing.TB, timeout, interval time.Duration, fn func() error) error {
	tb.Helper()
	if fn == nil {
		tb.Fatal("xtest.EventuallyEveryE: nil fn")
	}

	last := fn()
	if last == nil {
		return nil
	}
	if timeout <= 0 {
		return last
	}

	interval = normalizeInterval(interval)
	deadline := time.Now().Add(timeout)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return last
		}
		time.Sleep(min(interval, remaining))
		if last = fn(); last == nil {
			return nil
		}
	}
}

func Never(tb testing.TB, duration time.Duration, cond func() bool) {
	tb.Helper()
	if cond == nil {
		tb.Fatal("xtest.Never: nil condition")
	}

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if cond() {
			tb.Fatalf("xtest.Never: condition became true within %s", duration)
		}
		time.Sleep(DefaultPollInterval)
	}
}

func WaitForValue[T comparable](tb testing.TB, timeout time.Duration, want T, get func() T) T {
	tb.Helper()
	if get == nil {
		tb.Fatal("xtest.WaitForValue: nil get")
	}

	var last T
	deadline := time.Now().Add(timeout)

	for {
		last = get()
		if last == want {
			return last
		}
		if time.Now().After(deadline) {
			tb.Fatalf("xtest.WaitForValue: got %v, want %v after %s", last, want, timeout)
		}
		time.Sleep(DefaultPollInterval)
	}
}

func normalizeInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return DefaultPollInterval
	}
	return interval
}
