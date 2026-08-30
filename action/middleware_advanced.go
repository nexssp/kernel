package action

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// ── Singleflight (Deduplication) ──────────────────────────────────────────────

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

// ── Coalesce (Request Coalescing) ─────────────────────────────────────────────

type coalesceEntry struct {
	result any
	err    error
	ready  chan struct{}
}

type Coalescer struct {
	mu      sync.Mutex
	pending map[string]*coalesceEntry
}

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

// ── Adaptive (Circuit Breaker + Timeout) ──────────────────────────────────────

type AdaptiveConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	InitialTimeout   time.Duration
}

func (c *AdaptiveConfig) SetDefaults() {
	if c.FailureThreshold == 0 {
		c.FailureThreshold = 5
	}
	if c.ResetTimeout == 0 {
		c.ResetTimeout = 30 * time.Second
	}
	if c.InitialTimeout == 0 {
		c.InitialTimeout = 5 * time.Second
	}
}

type adaptiveState struct {
	mu          sync.RWMutex
	failures    int
	lastFailure time.Time
	open        bool
	timeout     time.Duration
	cfg         AdaptiveConfig
	name        string
}

func (a *adaptiveState) CurrentTimeout() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.timeout
}

func (a *adaptiveState) IsOpen() bool {
	a.mu.RLock()
	if !a.open {
		a.mu.RUnlock()
		return false
	}
	last := a.lastFailure
	timeout := a.cfg.ResetTimeout
	a.mu.RUnlock()

	if time.Since(last) > timeout {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.open {
			slog.Info("CircuitBreaker: Half-Open transition", "action", a.name)
			a.open = false
			a.failures = 0
		}
		return false
	}
	return true
}

func (a *adaptiveState) Observe(err error) {
	if err != nil && !xerr.IsTransient(err) {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.failures++
		a.lastFailure = time.Now()
		if a.failures >= a.cfg.FailureThreshold && !a.open {
			a.open = true
			a.timeout = time.Duration(float64(a.cfg.InitialTimeout) * (1 + float64(a.failures)/10))
			slog.Warn("CircuitBreaker: OPEN", "action", a.name, "failures", a.failures)
		}
		return
	}
	a.mu.Lock()
	if a.failures > 0 || a.open {
		a.failures = 0
		a.open = false
		a.timeout = a.cfg.InitialTimeout
	}
	a.mu.Unlock()
}

func Adaptive[Req, Res any](name string, cfg AdaptiveConfig) Middleware[Req, Res] {
	cfg.SetDefaults()
	state := &adaptiveState{
		timeout: cfg.InitialTimeout,
		cfg:     cfg,
		name:    name,
	}

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if state.IsOpen() {
				var zero Res
				return zero, xerr.CircuitBreaker("adaptive: circuit open")
			}

			tCtx, cancel := context.WithTimeout(ctx, state.CurrentTimeout())
			defer cancel()

			res, err := next(tCtx, req)

			if err != nil && ctx.Err() != nil {
				return res, err
			}

			state.Observe(err)
			return res, err
		}
	}
}
