// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"

	"golang.org/x/sync/singleflight"
)

type CacheLayer[V any] interface {
	Get(ctx context.Context, key string) (val V, hit bool, err error)
	Set(ctx context.Context, key string, val V, ttl time.Duration) error
}

type CacheConfig[Req, Res any] struct {
	KeyFunc func(Req) string
	Layers  []CacheLayer[Res]
	TTL     time.Duration
	Timeout time.Duration
}

// CacheMiddleware implements the Read-Through / Write-Behind pattern with isolated singleflight execution.
func CacheMiddleware[Req, Res any](cfg CacheConfig[Req, Res]) DispatcherMiddleware[Req, Res] {
	if cfg.KeyFunc == nil {
		panic("cache: KeyFunc must not be nil")
	}

	var sf singleflight.Group

	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			key := cfg.KeyFunc(req)
			if key == "" {
				return next(ctx, req)
			}
			if err := ctx.Err(); err != nil {
				var zero Res
				return zero, err
			}

			if val, hit := tryLayers(ctx, cfg, key, hooks, req); hit {
				return val, nil
			}

			return flightExecute(ctx, &sf, cfg, key, req, next, hooks)
		}
	}
}

func tryLayers[Req, Res any](
	ctx context.Context,
	cfg CacheConfig[Req, Res],
	key string,
	hooks HookDispatcher[Req, Res],
	req Req,
) (Res, bool) {
	for i, layer := range cfg.Layers {
		if layer == nil {
			continue
		}
		val, hit, err := layer.Get(ctx, key)
		if err != nil {
			slog.WarnContext(ctx, "cache_layer_get_failed",
				"key", key, "layer_index", i, "error", err)
			continue
		}
		if !hit {
			continue
		}
		hooks.OnCacheHit(ctx, req, val)
		backfillFasterLayers(ctx, cfg, key, val, i)
		return val, true
	}
	var zero Res
	return zero, false
}

func backfillFasterLayers[Req, Res any](
	ctx context.Context,
	cfg CacheConfig[Req, Res],
	key string,
	val Res,
	upTo int,
) {
	for j := range upTo {
		if cfg.Layers[j] == nil {
			continue
		}
		if err := cfg.Layers[j].Set(ctx, key, val, cfg.TTL); err != nil {
			slog.WarnContext(ctx, "cache_backfill_failed",
				"key", key, "layer_index", j, "error", err)
		}
	}
}

func flightExecute[Req, Res any](
	ctx context.Context,
	sf *singleflight.Group,
	cfg CacheConfig[Req, Res],
	key string,
	req Req,
	next Fn[Req, Res],
	hooks HookDispatcher[Req, Res],
) (Res, error) {
	baseCtx := context.WithoutCancel(ctx)
	var executedByThisCaller bool

	ch := sf.DoChan(key, func() (any, error) {
		executedByThisCaller = true
		return runFlight(baseCtx, cfg, key, req, next, hooks)
	})

	select {
	case <-ctx.Done():
		var zero Res
		return zero, ctx.Err()
	case result := <-ch:
		if err := ctx.Err(); err != nil {
			var zero Res
			return zero, err
		}
		if result.Shared && !executedByThisCaller {
			hooks.OnCoalesced(ctx, req)
		}
		if result.Err != nil {
			var zero Res
			return zero, result.Err
		}
		if result.Val == nil {
			var zero Res
			return zero, nil
		}
		res, ok := result.Val.(Res)
		if !ok {
			var zero Res
			return zero, fmt.Errorf("cache: unexpected stored type %T", result.Val)
		}
		return res, nil
	}
}

func runFlight[Req, Res any](
	baseCtx context.Context,
	cfg CacheConfig[Req, Res],
	key string,
	req Req,
	next Fn[Req, Res],
	hooks HookDispatcher[Req, Res],
) (result any, execErr error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	execCtx, cancel := context.WithTimeout(baseCtx, timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			execErr = xerr.PanicRecovery(r)
			result = nil
		}
	}()

	hooks.OnCacheMiss(execCtx, req)
	res, err := next(execCtx, req)
	if err != nil {
		return res, err
	}
	writeThroughAllLayers(execCtx, cfg, key, res)
	return res, nil
}

func writeThroughAllLayers[Req, Res any](
	ctx context.Context,
	cfg CacheConfig[Req, Res],
	key string,
	res Res,
) {
	for i, layer := range cfg.Layers {
		if layer == nil {
			continue
		}
		if err := layer.Set(ctx, key, res, cfg.TTL); err != nil {
			slog.WarnContext(ctx, "cache_write_through_failed",
				"key", key, "layer_index", i, "error", err)
		}
	}
}

const defaultCacheCapacity = 65536

type cacheItem[V any] struct {
	val V
	exp time.Time
}

type defaultMemoryKV[V any] struct {
	mu          sync.RWMutex
	data        map[string]cacheItem[V]
	ttl         time.Duration
	maxCapacity int
}

func newDefaultMemoryKV[V any](ttl time.Duration) *defaultMemoryKV[V] {
	return &defaultMemoryKV[V]{
		data:        make(map[string]cacheItem[V]),
		ttl:         ttl,
		maxCapacity: defaultCacheCapacity,
	}
}

//nolint:gocritic // (V, bool, error) is idiomatic Go; named results add noise
func (c *defaultMemoryKV[V]) Get(ctx context.Context, k string) (V, bool, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}

	now := time.Now()

	c.mu.RLock()
	item, ok := c.data[k]
	c.mu.RUnlock()

	if !ok {
		return zero, false, nil
	}

	if !item.exp.IsZero() && now.After(item.exp) {
		c.mu.Lock()
		if cur, ok := c.data[k]; ok && cur.exp.Equal(item.exp) {
			delete(c.data, k)
		}
		c.mu.Unlock()
		return zero, false, nil
	}

	return item.val, true, nil
}

func (c *defaultMemoryKV[V]) Set(ctx context.Context, k string, v V, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var exp time.Time
	now := time.Now()

	switch {
	case ttl > 0:
		exp = now.Add(ttl)
	case c.ttl > 0:
		exp = now.Add(c.ttl)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Bound memory: evict on overflow
	if len(c.data) >= c.maxCapacity {
		c.evictExpiredLocked(now)
		if len(c.data) >= c.maxCapacity {
			// Random replacement pass if map is still full
			for key := range c.data {
				delete(c.data, key)
				if len(c.data) < c.maxCapacity {
					break
				}
			}
		}
	}

	c.data[k] = cacheItem[V]{val: v, exp: exp}
	return nil
}

func (c *defaultMemoryKV[V]) Delete(ctx context.Context, k string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.data, k)
	c.mu.Unlock()
	return nil
}

func (c *defaultMemoryKV[V]) evictExpiredLocked(now time.Time) {
	for k, v := range c.data {
		if !v.exp.IsZero() && now.After(v.exp) {
			delete(c.data, k)
		}
	}
}
