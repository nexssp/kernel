package action

import (
	"context"
	"sync"

	"github.com/nexssp/kernel/xerr"
)

type inflightCall[Res any] struct {
	done chan struct{}
	res  Res
	err  error
}

type inflightMap[Res any] struct {
	mu sync.Mutex
	m  map[string]*inflightCall[Res]
}

func newInflightMap[Res any]() *inflightMap[Res] {
	return &inflightMap[Res]{m: make(map[string]*inflightCall[Res])}
}

func (f *inflightMap[Res]) Do(ctx context.Context, key string, fn func() (Res, error)) (val Res, shared bool, err error) {
	f.mu.Lock()
	if c, ok := f.m[key]; ok {
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			var zero Res
			return zero, false, ctx.Err()
		case <-c.done:
			return c.res, true, c.err
		}
	}

	c := &inflightCall[Res]{done: make(chan struct{})}
	f.m[key] = c
	f.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			c.err = xerr.PanicRecovery(r)
			var zero Res
			c.res = zero
		}
		f.mu.Lock()
		delete(f.m, key)
		f.mu.Unlock()
		close(c.done)
	}()

	c.res, c.err = fn()

	if ctx.Err() != nil {
		var zero Res
		return zero, false, ctx.Err()
	}
	return c.res, false, c.err
}

// Deduplicate returns a middleware that collapses concurrent identical requests
// into a single execution, sharing the result with all waiting callers.
func Deduplicate[Req, Res any](keyFn func(Req) string) DispatcherMiddleware[Req, Res] {
	c := newInflightMap[Res]()
	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			key := keyFn(req)
			if key == "" {
				return next(ctx, req)
			}

			detachedCtx := context.WithoutCancel(ctx)
			res, shared, err := c.Do(ctx, key, func() (Res, error) {
				return next(detachedCtx, req)
			})

			if shared {
				hooks.OnDeduplicated(ctx, req)
			}
			return res, err
		}
	}
}
