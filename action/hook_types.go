// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"log/slog"
	"reflect"
)

func callHook(meta *Meta, hook string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("action_hook_panic",
				"action", meta.Name,
				"hook", hook,
				"panic", r,
			)
		}
	}()
	fn()
}

type Hook[Req, Res any] struct {
	// OnBuild runs once per action at build time. Must be pure and O(1):
	// no I/O, no network. Return true to attach the hook to this action,
	// false to drop it — a dropped hook never enters the hook set and
	// pays zero cost in Do(). nil = always attach.
	//
	// reqType and resType are reflect.TypeFor[Req]() and
	// reflect.TypeFor[Res](). Both are compile-time constants.
	//
	// Filter by reqType when the hook depends on the shape of the input
	// (validation, input tracing). Filter by resType when it depends on
	// the shape of the output (cost reporting via CostReporter, output
	// validation, response tracing).
	//
	// For per-request decisions (feature flags, tenant policy, quota),
	// use Before instead — those belong on the hot path where ctx is
	// available.
	OnBuild        func(meta *Meta, reqType, resType reflect.Type) bool
	Before         func(ctx context.Context, req Req, meta *Meta) (context.Context, error)
	After          func(ctx context.Context, req Req, res Res, err error, meta *Meta)
	OnError        func(ctx context.Context, req Req, err error, meta *Meta)
	OnRetry        func(ctx context.Context, req Req, attempt int, err error, meta *Meta)
	OnCacheHit     func(ctx context.Context, req Req, res Res, meta *Meta)
	OnCacheMiss    func(ctx context.Context, req Req, meta *Meta)
	OnCoalesced    func(ctx context.Context, req Req, meta *Meta)
	OnDeduplicated func(ctx context.Context, req Req, meta *Meta)
	OnCancel       func(ctx context.Context, req Req, meta *Meta)
	OnExecuted     func(ctx context.Context, req Req, res Res, err error, meta *Meta)
}

// AnyHook is used for broad plugins (metrics, tracing, auth).
// Passing 'meta' dynamically guarantees thread-safety and zero allocations across shared instances.
type AnyHook struct {
	// OnBuild runs once per action at build time. Return true to attach
	// the hook to this action, false to drop it — a dropped hook never
	// enters the hook set and pays zero cost in Do().
	//
	// reqType and resType are reflect.TypeFor[Req]() and reflect.TypeFor[Res]().
	//
	// ctx comes from BuildContext / AddAnyHookContext / NewRegistryContext.
	// Build() and AddAnyHook() and NewRegistry() supply context.Background().
	// Build-time hooks are expected to be pure and O(1), but ctx is present
	// so that dynamic action construction inside a request can honor
	// cancellation and deadlines.
	//
	// nil = always attach.
	OnBuild        func(meta *Meta, reqType, resType reflect.Type) bool
	Before         func(ctx context.Context, req any, meta *Meta) (context.Context, error)
	After          func(ctx context.Context, req any, res any, err error, meta *Meta)
	OnError        func(ctx context.Context, req any, err error, meta *Meta)
	OnRetry        func(ctx context.Context, req any, attempt int, err error, meta *Meta)
	OnCacheHit     func(ctx context.Context, req any, res any, meta *Meta)
	OnCacheMiss    func(ctx context.Context, req any, meta *Meta)
	OnCoalesced    func(ctx context.Context, req any, meta *Meta)
	OnDeduplicated func(ctx context.Context, req any, meta *Meta)
	OnCancel       func(ctx context.Context, req any, meta *Meta)
	OnExecuted     func(ctx context.Context, req any, res any, err error, meta *Meta)
	OnPanic        func(ctx context.Context, req any, recovered any, meta *Meta)
}

// Adapt converts a type-safe Hook[Req, Res] into the standardized AnyHook container.
// It uses type assertions with safe zero-value fallbacks so hooks fire reliably even when res is nil.
func Adapt[Req, Res any](h Hook[Req, Res]) AnyHook {
	return AnyHook{
		OnBuild: h.OnBuild,
		Before: func(ctx context.Context, req any, meta *Meta) (context.Context, error) {
			if h.Before == nil {
				return ctx, nil
			}
			return h.Before(ctx, assertTo[Req](req), meta)
		},
		After: func(ctx context.Context, req any, res any, err error, meta *Meta) {
			if h.After == nil {
				return
			}
			h.After(ctx, assertTo[Req](req), assertTo[Res](res), err, meta)
		},
		OnError: func(ctx context.Context, req any, err error, meta *Meta) {
			if h.OnError == nil {
				return
			}
			h.OnError(ctx, assertTo[Req](req), err, meta)
		},
		OnExecuted: func(ctx context.Context, req any, res any, err error, meta *Meta) {
			if h.OnExecuted == nil || err != nil {
				return
			}
			h.OnExecuted(ctx, assertTo[Req](req), assertTo[Res](res), err, meta)
		},
		OnCacheHit: func(ctx context.Context, req any, res any, meta *Meta) {
			if h.OnCacheHit == nil {
				return
			}
			h.OnCacheHit(ctx, assertTo[Req](req), assertTo[Res](res), meta)
		},
		OnCacheMiss: func(ctx context.Context, req any, meta *Meta) {
			if h.OnCacheMiss == nil {
				return
			}
			h.OnCacheMiss(ctx, assertTo[Req](req), meta)
		},
		OnRetry: func(ctx context.Context, req any, attempt int, err error, meta *Meta) {
			if h.OnRetry == nil {
				return
			}
			h.OnRetry(ctx, assertTo[Req](req), attempt, err, meta)
		},
		OnCoalesced: func(ctx context.Context, req any, meta *Meta) {
			if h.OnCoalesced == nil {
				return
			}
			h.OnCoalesced(ctx, assertTo[Req](req), meta)
		},
		OnDeduplicated: func(ctx context.Context, req any, meta *Meta) {
			if h.OnDeduplicated == nil {
				return
			}
			h.OnDeduplicated(ctx, assertTo[Req](req), meta)
		},
		OnCancel: func(ctx context.Context, req any, meta *Meta) {
			if h.OnCancel == nil {
				return
			}
			h.OnCancel(ctx, assertTo[Req](req), meta)
		},
	}
}
