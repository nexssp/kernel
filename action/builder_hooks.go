package action

import "context"

// ── Fluent hook shortcuts ─────────────────────────────────────────────────────
// Each method wraps a single Hook field for common use cases.
// Use Hook(Hook[Req,Res]{...}) directly when you need multiple fields at once.

// HookBefore runs fn before the handler. Can enrich ctx or abort with an error.
// Aborting returns the error immediately — the handler never runs.
func (b *Builder[Req, Res]) HookBefore(fn func(ctx context.Context, req Req, meta *Meta) (context.Context, error)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{Before: fn})
}

// HookAfter runs fn after execution regardless of success or failure.
// Use for audit logging, always-on cleanup, unconditional metrics.
func (b *Builder[Req, Res]) HookAfter(fn func(ctx context.Context, req Req, res Res, err error, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{After: fn})
}

// HookError runs fn only when the handler returns a non-nil error.
// Use for alerting, dead-letter queues, structured error logging.
func (b *Builder[Req, Res]) HookError(fn func(ctx context.Context, req Req, err error, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnError: fn})
}

// HookExecuted runs fn only on successful execution (err == nil).
// The fn signature omits err for convenience — it is always nil here.
// Use for domain-event publishing, analytics, cache warming.
func (b *Builder[Req, Res]) HookExecuted(fn func(ctx context.Context, req Req, res Res, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{
		OnExecuted: func(ctx context.Context, req Req, res Res, err error, meta *Meta) {
			if err == nil {
				fn(ctx, req, res, meta)
			}
		},
	})
}

// HookCancel runs fn when the request context is canceled
// (client disconnect, upstream timeout, explicit cancel).
// Use for cleanup, releasing reserved resources, cancellation metrics.
func (b *Builder[Req, Res]) HookCancel(fn func(ctx context.Context, req Req, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnCancel: fn})
}

// HookRetry runs fn before each retry attempt made by the Retry middleware.
// attempt starts at 1 for the first retry.
// Use for retry-specific logging, jitter metrics, backoff tracing.
func (b *Builder[Req, Res]) HookRetry(fn func(ctx context.Context, req Req, attempt int, err error, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnRetry: fn})
}

// HookCacheHit runs fn when the CacheMiddleware serves a response from cache.
// The handler is NOT called in this case.
// Use for cache-hit metrics, hit-rate logging.
func (b *Builder[Req, Res]) HookCacheHit(fn func(ctx context.Context, req Req, res Res, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnCacheHit: fn})
}

// HookCacheMiss runs fn when the CacheMiddleware finds no cached result.
// The handler will be called immediately after.
// Use for cache-miss metrics, warming triggers.
func (b *Builder[Req, Res]) HookCacheMiss(fn func(ctx context.Context, req Req, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnCacheMiss: fn})
}

// HookRegister runs fn once at Build() time — never on the hot path.
// Use to pre-allocate metric labels, register with service discovery,
// validate configuration, or build static routing tables.
//
// Example — pre-allocate Prometheus labels:
//
//	.HookRegister(func(meta *action.Meta) {
//	    requestCounter.WithLabelValues(meta.Name) // pre-allocate label set
//	})
func (b *Builder[Req, Res]) HookRegister(fn func(meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnRegister: fn})
}

// ── AnyHook registration ──────────────────────────────────────────────────────

// AnyHook registers a type-erased hook (used by plugins: monitor, telemetry, tracing).
// Prefer typed Hook[Req,Res] when the action types are known.
func (b *Builder[Req, Res]) AnyHook(h ...AnyHook) *Builder[Req, Res] {
	b.anyHooks = append(b.anyHooks, h...)
	return b
}

// ── Composition helpers ───────────────────────────────────────────────────────

// Hook registers a full typed hook struct.
// Use when you need more than one hook event in a single declaration.
func (b *Builder[Req, Res]) Hook(h ...Hook[Req, Res]) *Builder[Req, Res] {
	b.hooks = append(b.hooks, h...)
	return b
}

// Add composes AnyHooks and Bindings from other AnyActions (plugins) into this builder.
// Typed hooks are NOT composed — use Hook() directly for those.
// Safe to call multiple times; idempotent per unique plugin instance.
func (b *Builder[Req, Res]) Add(others ...AnyAction) *Builder[Req, Res] {
	for _, o := range others {
		if o == nil {
			continue
		}
		b.anyHooks = append(b.anyHooks, o.GetAnyHooks()...)
		b.bindings = append(b.bindings, o.GetBindings()...)
	}
	return b
}

// Compose copies typed hooks, AnyHooks, and Bindings from another BuiltAction
// with the same Req/Res types into this builder.
func (b *Builder[Req, Res]) Compose(other *BuiltAction[Req, Res]) *Builder[Req, Res] {
	if other == nil {
		return b
	}

	b.hooks = append(b.hooks, other.hooks...)
	b.anyHooks = append(b.anyHooks, other.GetAnyHooks()...)
	b.bindings = append(b.bindings, other.bindings...)

	// Inherit operational metadata from the composed action.
	// Do not overwrite values explicitly set on this builder.
	if other.meta != nil {
		if !b.meta.Idempotency.Enabled && other.meta.Idempotency.Enabled {
			b.meta.Idempotency = other.meta.Idempotency
		}

		if b.meta.SuccessStatus == 0 && other.meta.SuccessStatus != 0 {
			b.meta.SuccessStatus = other.meta.SuccessStatus
		}

		// Optionally inherit more operational metadata:
		// Timeout, RetryMax, ConcurrencyLimit, CacheTTL,
		// RequiredRoles, RequiredPermissions, RequiresAuth, etc.
	}

	return b
}
