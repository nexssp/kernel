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
			slog.Error("action_hook_panic", "action", meta.Name, "hook", hook, "panic", r)
		}
	}()
	fn()
}

// Hook is a strongly typed hook set for one request/response pair.
// All fields are optional. Hook panics are isolated by the action runtime.
type Hook[Req, Res any] struct {
	OnBuild func(meta *Meta, reqType, resType reflect.Type) bool

	Before func(ctx context.Context, req Req, meta *Meta) (context.Context, error)
	After  func(ctx context.Context, req Req, res Res, err error, meta *Meta)

	// OnSuccess fires after a successful real execution. It does not fire for
	// a result returned directly from cache.
	OnSuccess func(ctx context.Context, req Req, res Res, meta *Meta)

	// OnTimeout fires before OnError when the returned error matches
	// context.DeadlineExceeded.
	OnTimeout func(ctx context.Context, req Req, meta *Meta)
	OnError   func(ctx context.Context, req Req, err error, meta *Meta)

	OnRetry        func(ctx context.Context, req Req, attempt int, err error, meta *Meta)
	OnCacheHit     func(ctx context.Context, req Req, res Res, meta *Meta)
	OnCacheMiss    func(ctx context.Context, req Req, meta *Meta)
	OnCoalesced    func(ctx context.Context, req Req, meta *Meta)
	OnDeduplicated func(ctx context.Context, req Req, meta *Meta)
	OnCancel       func(ctx context.Context, req Req, meta *Meta)
}

// AnyHook is the type-erased equivalent of Hook[Req, Res]. OnPanic is kept
// here because recovered panic values are not representable by typed hooks.
type AnyHook struct {
	OnBuild func(meta *Meta, reqType, resType reflect.Type) bool

	Before func(ctx context.Context, req any, meta *Meta) (context.Context, error)
	After  func(ctx context.Context, req any, res any, err error, meta *Meta)

	OnSuccess func(ctx context.Context, req any, res any, meta *Meta)
	OnTimeout func(ctx context.Context, req any, meta *Meta)
	OnError   func(ctx context.Context, req any, err error, meta *Meta)

	OnRetry        func(ctx context.Context, req any, attempt int, err error, meta *Meta)
	OnCacheHit     func(ctx context.Context, req any, res any, meta *Meta)
	OnCacheMiss    func(ctx context.Context, req any, meta *Meta)
	OnCoalesced    func(ctx context.Context, req any, meta *Meta)
	OnDeduplicated func(ctx context.Context, req any, meta *Meta)
	OnCancel       func(ctx context.Context, req any, meta *Meta)

	OnPanic func(ctx context.Context, req any, recovered any, meta *Meta)
}

// Adapt converts a typed hook to its type-erased representation.
func Adapt[Req, Res any](h Hook[Req, Res]) AnyHook {
	ah := AnyHook{OnBuild: h.OnBuild}

	if h.Before != nil {
		ah.Before = func(ctx context.Context, req any, meta *Meta) (context.Context, error) {
			return h.Before(ctx, assertTo[Req](req), meta)
		}
	}
	if h.After != nil {
		ah.After = func(ctx context.Context, req any, res any, err error, meta *Meta) {
			h.After(ctx, assertTo[Req](req), assertTo[Res](res), err, meta)
		}
	}
	if h.OnSuccess != nil {
		ah.OnSuccess = func(ctx context.Context, req any, res any, meta *Meta) {
			h.OnSuccess(ctx, assertTo[Req](req), assertTo[Res](res), meta)
		}
	}
	if h.OnTimeout != nil {
		ah.OnTimeout = func(ctx context.Context, req any, meta *Meta) {
			h.OnTimeout(ctx, assertTo[Req](req), meta)
		}
	}
	if h.OnError != nil {
		ah.OnError = func(ctx context.Context, req any, err error, meta *Meta) {
			h.OnError(ctx, assertTo[Req](req), err, meta)
		}
	}
	if h.OnRetry != nil {
		ah.OnRetry = func(ctx context.Context, req any, attempt int, err error, meta *Meta) {
			h.OnRetry(ctx, assertTo[Req](req), attempt, err, meta)
		}
	}
	if h.OnCacheHit != nil {
		ah.OnCacheHit = func(ctx context.Context, req any, res any, meta *Meta) {
			h.OnCacheHit(ctx, assertTo[Req](req), assertTo[Res](res), meta)
		}
	}
	if h.OnCacheMiss != nil {
		ah.OnCacheMiss = func(ctx context.Context, req any, meta *Meta) {
			h.OnCacheMiss(ctx, assertTo[Req](req), meta)
		}
	}
	if h.OnCoalesced != nil {
		ah.OnCoalesced = func(ctx context.Context, req any, meta *Meta) {
			h.OnCoalesced(ctx, assertTo[Req](req), meta)
		}
	}
	if h.OnDeduplicated != nil {
		ah.OnDeduplicated = func(ctx context.Context, req any, meta *Meta) {
			h.OnDeduplicated(ctx, assertTo[Req](req), meta)
		}
	}
	if h.OnCancel != nil {
		ah.OnCancel = func(ctx context.Context, req any, meta *Meta) {
			h.OnCancel(ctx, assertTo[Req](req), meta)
		}
	}
	return ah
}
