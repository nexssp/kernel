package action

import (
	"context"
	"fmt"
	"log/slog"
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
