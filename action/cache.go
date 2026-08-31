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

			// 1. Try layers L1 -> LN in order
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

				if hit {
					hooks.OnCacheHit(ctx, req, val)

					// Backfill all faster layers (0 to i-1)
					if i > 0 {
						for j := 0; j < i; j++ {
							if cfg.Layers[j] == nil {
								continue
							}
							if fillErr := cfg.Layers[j].Set(ctx, key, val, cfg.TTL); fillErr != nil {
								slog.WarnContext(ctx, "cache_backfill_failed",
									"key", key, "layer_index", j, "error", fillErr)
							}
						}
					}
					return val, nil
				}
			}

			// 2. Singleflight execution with a context owned by the flight,
			//    independent of any individual caller's cancellation.
			baseCtx := context.WithoutCancel(ctx)
			var executedByThisCaller bool

			ch := sf.DoChan(key, func() (result any, execErr error) {
				executedByThisCaller = true

				flightTimeout := cfg.Timeout
				if flightTimeout <= 0 {
					flightTimeout = 2 * time.Minute
				}

				execCtx, execCancel := context.WithTimeout(baseCtx, flightTimeout)
				defer execCancel()

				defer func() {
					if r := recover(); r != nil {
						execErr = xerr.PanicRecovery(r)
						result = nil
					}
				}()

				hooks.OnCacheMiss(execCtx, req)

				res, err := next(execCtx, req)
				if err == nil {
					for idx, layer := range cfg.Layers {
						if layer == nil {
							continue
						}
						if writeErr := layer.Set(execCtx, key, res, cfg.TTL); writeErr != nil {
							slog.WarnContext(execCtx, "cache_write_through_failed",
								"key", key, "layer_index", idx, "error", writeErr)
						}
					}
				}
				return res, err
			})

			// 3. Wait for result.
			select {
			case <-ctx.Done():
				var zero Res
				return zero, ctx.Err()

			case result := <-ch:
				// Required: even if the flight completed, the caller must still
				// observe its own cancellation if its context was canceled.
				if err := ctx.Err(); err != nil {
					var zero Res
					return zero, err
				}

				// Only notify joining waiters.
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
	}
}
