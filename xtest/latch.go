package xtest

import (
	"sync"
	"testing"
	"time"
)

// Latch is a one-shot signal for tests. Signal is idempotent and safe for
// concurrent use; Wait blocks until Signal has been called or the timeout
// elapses.
type Latch struct {
	once sync.Once
	ch   chan struct{}
}

func NewLatch() *Latch { return &Latch{ch: make(chan struct{})} }

func (l *Latch) Signal() { l.once.Do(func() { close(l.ch) }) }

func (l *Latch) Wait(tb testing.TB, timeout time.Duration) {
	tb.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-l.ch:
	case <-timer.C:
		tb.Fatal("xtest.Latch: not signaled within timeout")
	}
}
