// Package xsync provides strongly typed concurrency primitives.
package xsync

import "sync"

// Pool is a generic wrapper around sync.Pool. It enforces zero-allocation
// object reuse by eliminating the interface{} (any) boxing and type assertion
// overhead required by the standard library pool.
type Pool[T any] struct {
	pool sync.Pool
}

// NewPool creates a new typed pool. The factory must return a pointer.
func NewPool[T any](factory func() *T) *Pool[T] {
	return &Pool[T]{
		pool: sync.Pool{
			New: func() any {
				return factory()
			},
		},
	}
}

// Get retrieves a pooled item.
func (p *Pool[T]) Get() *T {
	val := p.pool.Get()
	if item, ok := val.(*T); ok {
		return item
	}
	return nil
}

// Put returns an item to the pool. Callers MUST reset the item's state
// before returning it to avoid data contamination.
func (p *Pool[T]) Put(item *T) {
	p.pool.Put(item)
}
