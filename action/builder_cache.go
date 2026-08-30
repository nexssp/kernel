package action

// builder_cache.go — Fluent cache wiring on Builder[Req, Res].
//
// Eliminates the CacheConfig wrapper for common cases.
// Type parameters are inferred from the receiver — callers write no generics.
//
// Before:
//
//	action.New("product.get", fetchProduct).
//	    UseWithDispatcher(action.CacheMiddleware(action.CacheConfig[ProductReq, Product]{
//	        KeyFunc: func(req ProductReq) string { return req.ProductID },
//	        Layers:  []action.Cache[Product]{l1, l2},
//	        TTL:     10 * time.Minute,
//	    })).Build()
//
// After:
//
//	action.New("product.get", fetchProduct).
//	    Cache(10*time.Minute, func(r ProductReq) string { return r.ProductID }, l1, l2).
//	    Build()

import (
	"context"
	"sync"
	"time"
)

// Cache adds a multi-layer cache to the action.
// Layers are checked in order: L1 (fast, local) → L2 (shared, e.g. Redis).
// On a miss, the handler runs and the result is written to all layers.
// On an L2 hit, L1 is back-filled automatically.
//
// The keyFn derives a stable string cache key from the request.
// Keep keys short and deterministic (UUID, int, composite "tenant:id", etc.).
//
// Example — single in-memory layer:
//
//	action.New("catalog.list", fetchCatalog).
//	    Cache(30*time.Minute,
//	        func(_ CatalogReq) string { return "catalog:all" },
//	        cache.NewInMemory[[]Product](30*time.Minute),
//	    ).Build()
//
// Example — L1 memory + L2 Redis:
//
//	action.New("product.get", fetchProduct).
//	    Cache(10*time.Minute,
//	        func(r ProductReq) string { return r.ProductID },
//	        cache.NewInMemory[Product](5*time.Minute),
//	        cache.NewRedis[Product](redisClient),
//	    ).Build()
func (b *Builder[Req, Res]) Cache(ttl time.Duration, keyFn func(Req) string, layers ...CacheLayer[Res]) *Builder[Req, Res] {
	b.meta.CacheTTL = ttl
	if len(layers) == 0 {
		layers = []CacheLayer[Res]{newDefaultMemoryKV[Res](ttl)}
	}
	return b.UseWithDispatcher(CacheMiddleware(CacheConfig[Req, Res]{
		KeyFunc: keyFn,
		Layers:  layers,
		TTL:     ttl,
	}))
}

// Once caches the result of the first successful execution and returns it
// for all subsequent calls. Errors are also cached – the action will not retry.
// Use only for idempotent, read‑only actions (e.g., static config generation).
//
// The first caller determines the cached result; later callers receive the same
// value or error without re‑executing the handler. Context cancellation is
// ignored after the first execution.
//
// Example:
//
//	action.New("config.generate", generator).
//	    Once().
//	    Route(thttp.GET("/config.json")).
//	    Build()
func (b *Builder[Req, Res]) Once() *Builder[Req, Res] {
	var once sync.Once
	var cachedRes Res
	var cachedErr error

	b.meta.CacheTTL = -1

	return b.UseWithDispatcher(func(next Fn[Req, Res], _ HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			once.Do(func() {
				cachedRes, cachedErr = next(ctx, req)
			})
			return cachedRes, cachedErr
		}
	})
}
