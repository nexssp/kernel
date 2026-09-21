package xtest

import (
	"testing"
	"time"
)

// WaitForSignal waits for a one-shot signal and fails with a useful timeout.
func WaitForSignal(tb testing.TB, signal <-chan struct{}, timeout time.Duration) {
	tb.Helper()
	if signal == nil {
		tb.Fatal("xtest.WaitForSignal: nil channel")
	}
	if timeout <= 0 {
		tb.Fatalf("xtest.WaitForSignal: timeout must be positive, got %s", timeout)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		tb.Fatalf("xtest.WaitForSignal: signal not received within %s", timeout)
	}
}
