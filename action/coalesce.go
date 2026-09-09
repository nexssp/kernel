package action

import (
	"context"
	"fmt"
	"sync"

	"github.com/nexssp/kernel/xerr"
)

type coalesceEntry struct {
	result any
	err    error
	ready  chan struct{}
}

// Coalescer coordinates in-flight request sharing across different actions or callers.
type Coalescer struct {
	mu      sync.Mutex
	pending map[string]*coalesceEntry
}

// NewCoalescer creates a new request coalescing coordinator.
func NewCoalescer() *Coalescer {
	return &Coalescer{pending: make(map[string]*coalesceEntry)}
}

func (c *Coalescer) Do(ctx context.Context, key string, fn func(context.Context) (any, error)) (val any, shared bool, err error) {
	c.mu.Lock()
	if entry, ok := c.pending[key]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-entry.ready:
			return entry.result, true, entry.err
		}
	}

	entry := &coalesceEntry{ready: make(chan struct{})}
	c.pending[key] = entry
	c.mu.Unlock()

	var result any
	var execErr error

	defer func() {
		if r := recover(); r != nil {
			execErr = xerr.PanicRecovery(r)
			result = nil
			c.mu.Lock()
			entry.result = nil
			entry.err = execErr
			c.mu.Unlock()
		}
		c.mu.Lock()
		if _, exists := c.pending[key]; exists {
			close(entry.ready)
			delete(c.pending, key)
		}
		c.mu.Unlock()
	}()

	result, execErr = fn(ctx)

	c.mu.Lock()
	entry.result = result
	entry.err = execErr
	c.mu.Unlock()

	return result, false, execErr
}

// CoalesceMiddleware wraps an action with shared request coalescing.
func CoalesceMiddleware[Req, Res any](coalescer *Coalescer, actionName string, keyFn func(Req) string) DispatcherMiddleware[Req, Res] {
	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			key := keyFn(req)
			if key == "" {
				return next(ctx, req)
			}

			fullKey := fmt.Sprintf("%s:%s", actionName, key)
			detachedCtx := context.WithoutCancel(ctx)

			val, shared, err := coalescer.Do(ctx, fullKey, func(_ context.Context) (any, error) {
				return next(detachedCtx, req)
			})

			if shared {
				hooks.OnCoalesced(ctx, req)
			}

			if err != nil {
				var zero Res
				return zero, err
			}
			res, ok := val.(Res)
			if !ok {
				var zero Res
				return zero, fmt.Errorf("%s: coalesced result type mismatch: got %T", actionName, val)
			}
			return res, nil
		}
	}
}
