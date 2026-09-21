// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

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

// builder_dedup.go — Request deduplication shorthands on Builder[Req, Res].
//
// Both methods infer Req and Res from the builder receiver.
// Callers write zero type annotations.

// Dedup prevents concurrent identical requests from executing multiple times.
// All callers with the same key block until the first one completes,
// then share its result — exactly one handler invocation per unique key
// at any point in time.
//
// keyFn returns the dedup key. An empty string disables dedup for that request.
//
// Use case — prevent N concurrent callers from all hitting the DB for the
// same product:
//
//	productAct := action.New("product.price", fetchPrice).
//	    Dedup(func(r PriceReq) string { return r.ProductID }).
//	    Build()
func (b *Builder[Req, Res]) Dedup(keyFn func(Req) string) *Builder[Req, Res] {
	b.meta.Deduplicated = true
	return b.UseWithDispatcher(Deduplicate[Req, Res](keyFn))
}

// Coalesce deduplicates concurrent requests using request coalescing.
// Unlike Dedup, Coalesce allows a caller whose context is canceled
// to bail out early without killing the underlying in-flight request —
// the in-flight request continues for other waiters.
//
// A single *Coalescer can be shared across multiple actions:
//
//	c := action.NewCoalescer()
//
//	productAct := action.New("product.get", fetchProduct).
//	    Coalesce(c, func(r ProductReq) string { return r.ProductID }).
//	    Build()
//
//	inventoryAct := action.New("inventory.check", checkStock).
//	    Coalesce(c, func(r InventoryReq) string { return r.SKU }).
//	    Build()
func (b *Builder[Req, Res]) Coalesce(c *Coalescer, keyFn func(Req) string) *Builder[Req, Res] {
	b.meta.Coalesced = true
	name := b.meta.Name
	return b.UseWithDispatcher(CoalesceMiddleware[Req, Res](c, name, keyFn))
}
