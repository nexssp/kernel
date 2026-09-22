// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"reflect"
)

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

// HookSuccess runs fn only after a successful real execution.
// Use for domain-event publishing, analytics, cache warming.
func (b *Builder[Req, Res]) HookSuccess(fn func(ctx context.Context, req Req, res Res, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{
		OnSuccess: fn,
	})
}

// HookSuccessEvent runs fn after a successful real execution, except when the
// result is served directly from cache.
func (b *Builder[Req, Res]) HookSuccessEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnSuccess: func(context.Context, Req, Res, *Meta) { fn() },
	})
}

// HookTimeout runs fn when execution returns context.DeadlineExceeded.
func (b *Builder[Req, Res]) HookTimeout(fn func(ctx context.Context, req Req, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnTimeout: fn})
}

// HookTimeoutEvent runs fn when execution returns context.DeadlineExceeded.
func (b *Builder[Req, Res]) HookTimeoutEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnTimeout: func(context.Context, Req, *Meta) { fn() },
	})
}

// HookCancel runs fn when the request context is canceled
// (client disconnect, upstream timeout, explicit cancel).
// Use for cleanup, releasing reserved resources, cancellation metrics.
func (b *Builder[Req, Res]) HookCancel(fn func(ctx context.Context, req Req, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnCancel: fn})
}

// HookCancelEvent runs fn when the request is canceled.
func (b *Builder[Req, Res]) HookCancelEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnCancel: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookRetry runs fn before each retry attempt made by the Retry middleware.
// attempt starts at 1 for the first retry.
// Use for retry-specific logging, jitter metrics, backoff tracing.
func (b *Builder[Req, Res]) HookRetry(fn func(ctx context.Context, req Req, attempt int, err error, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnRetry: fn})
}

// HookRetryEvent runs fn before each retry attempt.
func (b *Builder[Req, Res]) HookRetryEvent(fn func(attempt int, err error)) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnRetry: func(_ context.Context, _ Req, attempt int, err error, _ *Meta) {
			fn(attempt, err)
		},
	})
}

// HookCacheHit runs fn when the CacheMiddleware serves a response from cache.
// The handler is NOT called in this case.
// Use for cache-hit metrics, hit-rate logging.
func (b *Builder[Req, Res]) HookCacheHit(fn func(ctx context.Context, req Req, res Res, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnCacheHit: fn})
}

// HookCacheHitEvent runs fn when the cache serves a response.
func (b *Builder[Req, Res]) HookCacheHitEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnCacheHit: func(_ context.Context, _ Req, _ Res, _ *Meta) { fn() },
	})
}

// HookCacheMiss runs fn when the CacheMiddleware finds no cached result.
// The handler will be called immediately after.
// Use for cache-miss metrics, warming triggers.
func (b *Builder[Req, Res]) HookCacheMiss(fn func(ctx context.Context, req Req, meta *Meta)) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnCacheMiss: fn})
}

// HookCacheMissEvent runs fn when the cache has no response for a request.
func (b *Builder[Req, Res]) HookCacheMissEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnCacheMiss: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookErrorEvent runs fn when execution returns an error.
func (b *Builder[Req, Res]) HookErrorEvent(fn func(error)) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnError: func(_ context.Context, _ Req, err error, _ *Meta) { fn(err) },
	})
}

// HookCoalescedEvent runs fn when this call joins an in-flight coalesced call.
func (b *Builder[Req, Res]) HookCoalescedEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnCoalesced: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookDeduplicatedEvent runs fn when a duplicate call is suppressed.
func (b *Builder[Req, Res]) HookDeduplicatedEvent(fn func()) *Builder[Req, Res] {
	if fn == nil {
		return b
	}
	return b.Hook(Hook[Req, Res]{
		OnDeduplicated: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookBuild runs fn once per action at build time. Must be pure and O(1):
// no I/O, no network. Return true to attach the hook, false to drop it —
// a dropped hook pays zero cost in Do().
//
// reqType and resType are reflect.TypeFor[Req]() and reflect.TypeFor[Res]().
// Use them to filter the hook to a subset of actions.
//
// For per-request decisions (feature flags, tenant policy, quota), use
// the runtime Before callback instead.
//
// Example — attach a cost ledger hook only to actions whose response
// reports cost:
//
//	.HookBuild(func(meta *action.Meta, _, resType reflect.Type) bool {
//	    if !resType.Implements(reflect.TypeFor[CostReporter]()) {
//	        return false
//	    }
//	    ledger.Register(meta.Name)
//	    return true
//	})
func (b *Builder[Req, Res]) HookBuild(
	fn func(meta *Meta, reqType, resType reflect.Type) bool,
) *Builder[Req, Res] {
	return b.Hook(Hook[Req, Res]{OnBuild: fn})
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

// Compose copies runtime hooks (typed + AnyHook), bindings, and transport
// hints (Idempotency, SuccessStatus) from a compiled action into this builder.
//
// Middleware-backed policies are intentionally NOT inherited. Timeout, RetryMax,
// ConcurrencyLimit, CacheTTL, RequiresAuth, and the Required* guards are
// already compiled into other.exec as closures and cannot be extracted from a
// BuiltAction.
func (b *Builder[Req, Res]) Compose(other *BuiltAction[Req, Res]) *Builder[Req, Res] {
	if other == nil {
		return b
	}

	b.hooks = append(b.hooks, other.hooks...)
	b.anyHooks = append(b.anyHooks, other.GetAnyHooks()...)
	b.bindings = append(b.bindings, other.bindings...)

	// Transport hints — no middleware needed, safe to copy.
	// Do not overwrite values explicitly set on this builder.
	if other.meta != nil {
		if !b.meta.Idempotency.Enabled && other.meta.Idempotency.Enabled {
			b.meta.Idempotency = other.meta.Idempotency
		}
		if b.meta.SuccessStatus == 0 && other.meta.SuccessStatus != 0 {
			b.meta.SuccessStatus = other.meta.SuccessStatus
		}
	}

	return b
}
