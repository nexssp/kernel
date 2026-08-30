package action

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
