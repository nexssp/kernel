package xtest

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Latch is a one-shot signal for tests. Zero value is not usable — always
// construct via NewLatch. Signal is idempotent and safe for concurrent use;
// Wait blocks until Signal has been called or the timeout elapses.
type Latch struct {
	once sync.Once
	ch   chan struct{}
}

func NewLatch() *Latch { return &Latch{ch: make(chan struct{})} }

func (l *Latch) Signal() { l.once.Do(func() { close(l.ch) }) }

// Done returns a receive-only channel closed when Signal is called.
func (l *Latch) Done() <-chan struct{} { return l.ch }

// Wait blocks until Signal has been called or the timeout elapses.
func (l *Latch) Wait(tb testing.TB, timeout time.Duration) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := l.WaitContext(ctx); err != nil {
		tb.Fatalf("xtest.Latch: not signaled within %s", timeout)
	}
}

// WaitContext blocks until Signal is called or ctx is canceled.
func (l *Latch) WaitContext(ctx context.Context) error {
	select {
	case <-l.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
