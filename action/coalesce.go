// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nexssp/kernel/xerr"
)

type coalesceEntry struct {
	result  any
	err     error
	ready   chan struct{}
	waiters atomic.Int32
}

type coalesceKey struct {
	action     string
	key        string
	namespaced bool
}

// Coalescer coordinates in-flight request sharing across different actions or callers.
type Coalescer struct {
	mu      sync.Mutex
	pending map[coalesceKey]*coalesceEntry
}

// NewCoalescer creates a new request coalescing coordinator.
func NewCoalescer() *Coalescer {
	return &Coalescer{pending: make(map[coalesceKey]*coalesceEntry)}
}

func (c *Coalescer) Do(ctx context.Context, key string, fn func(context.Context) (any, error)) (val any, shared bool, err error) {
	return c.do(ctx, coalesceKey{key: key}, fn)
}

func (c *Coalescer) do(ctx context.Context, key coalesceKey, fn func(context.Context) (any, error)) (val any, shared bool, err error) {
	c.mu.Lock()
	if entry, ok := c.pending[key]; ok {
		entry.waiters.Add(1)
		defer entry.waiters.Add(-1)
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
			// Waiters read entry.*, the leader's caller reads the
			// named returns — set them or the panic is dropped.
			val = nil
			shared = false
			err = execErr
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

// Waiters reports the number of callers waiting on the given key.
// It runs in O(pending size); use it for diagnostics, not hot paths.
func (c *Coalescer) Waiters(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var waiters int
	for pendingKey, entry := range c.pending {
		if !pendingKey.namespaced && pendingKey.key == key || pendingKey.namespaced && pendingKey.action+":"+pendingKey.key == key {
			waiters += int(entry.waiters.Load())
		}
	}
	return waiters
}

// CoalesceMiddleware wraps an action with shared request coalescing and the default flight timeout.
func CoalesceMiddleware[Req, Res any](coalescer *Coalescer, actionName string, keyFn func(Req) string) DispatcherMiddleware[Req, Res] {
	return CoalesceMiddlewareWithOptions[Req, Res](coalescer, actionName, keyFn, SharedFlightOptions{})
}

// CoalesceMiddlewareWithOptions configures the shared work's context deadline.
// The shared work keeps the leader's context values but not its cancellation; a
// waiting caller's cancellation only ends that caller's wait.
func CoalesceMiddlewareWithOptions[Req, Res any](
	coalescer *Coalescer,
	actionName string,
	keyFn func(Req) string,
	options SharedFlightOptions,
) DispatcherMiddleware[Req, Res] {
	timeout := options.withDefaults().Timeout
	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			key := keyFn(req)
			if key == "" {
				return next(ctx, req)
			}

			val, shared, err := coalescer.do(ctx, coalesceKey{action: actionName, key: key, namespaced: true}, func(leaderCtx context.Context) (any, error) {
				flightCtx, cancel := withSharedFlightTimeout(leaderCtx, timeout)
				if cancel != nil {
					defer cancel()
				}
				res, err := next(flightCtx, req)
				if err != nil {
					return res, err
				}
				if flightErr := flightCtx.Err(); flightErr != nil {
					return nil, flightErr
				}
				return res, nil
			})
			if err == nil && !shared && ctx.Err() != nil {
				var zero Res
				return zero, ctx.Err()
			}

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
