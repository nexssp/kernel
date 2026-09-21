package xtest

import (
	"sync/atomic"
)

// Gate provides a concurrent barrier for thundering-herd tests.
type Gate struct {
	ready  chan struct{}
	active atomic.Int32
}

// NewGate returns a new synchronization barrier.
func NewGate() *Gate {
	return &Gate{ready: make(chan struct{})}
}

// Enter blocks until Release is called.
func (g *Gate) Enter() {
	g.active.Add(1)
	<-g.ready
}

// Release unblocks all goroutines waiting in Enter.
func (g *Gate) Release() {
	close(g.ready)
}

// Active returns the number of goroutines that have called Enter.
func (g *Gate) Active() int32 {
	return g.active.Load()
}
