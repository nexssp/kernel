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
	Layers  []CacheLayer[Res] // L1 (In-Memory) -> L2 (NATS/Redis)
	TTL     time.Duration
}

// CacheMiddleware implements the Read-Through / Write-Behind pattern
func CacheMiddleware[Req, Res any](cfg CacheConfig[Req, Res]) DispatcherMiddleware[Req, Res] {
	var sf singleflight.Group
	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			key := cfg.KeyFunc(req)
			if key == "" {
				return next(ctx, req)
			}
			// Try layers L1 → L2 (order matters!)
			for i, layer := range cfg.Layers {
				val, hit, err := layer.Get(ctx, key)
				if err != nil {
					// Log infrastructure degradation but do not crash the request
					slog.WarnContext(ctx, "cache_layer_get_failed",
						"key", key, "layer_index", i, "error", err)
					continue
				}
				if hit {
					hooks.OnCacheHit(ctx, req, val)

					if i > 0 && len(cfg.Layers) > 0 {
						if fillErr := cfg.Layers[0].Set(ctx, key, val, cfg.TTL); fillErr != nil {
							slog.WarnContext(ctx, "cache_backfill_failed",
								"key", key, "error", fillErr)
						}
					}
					return val, nil
				}
			}

			hooks.OnCacheMiss(ctx, req)

			v, err, _ := sf.Do(key, func() (result any, execErr error) {
				defer func() {
					if r := recover(); r != nil {
						execErr = xerr.PanicRecovery(r)
						result = nil
					}
				}()
				res, err := next(ctx, req)
				if err == nil {
					for _, layer := range cfg.Layers {
						if writeErr := layer.Set(ctx, key, res, cfg.TTL); writeErr != nil {
							slog.WarnContext(ctx, "cache_write_through_failed",
								"key", key, "error", writeErr)
						}
					}
				}
				return res, err
			})

			if err != nil {
				var zero Res
				return zero, err
			}
			res, ok := v.(Res)
			if !ok {
				var zero Res
				return zero, fmt.Errorf("cache: unexpected stored type %T", v)
			}
			return res, nil
		}
	}
}
