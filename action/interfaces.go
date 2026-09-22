// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
)

type HookProvider interface {
	GetAnyHooks() []AnyHook
}

//nolint:revive // exported name kept for API stability
type ActionProvider interface {
	Actions() []AnyAction
}

type Fn[Req, Res any] func(context.Context, Req) (Res, error)

type Middleware[Req, Res any] func(Fn[Req, Res]) Fn[Req, Res]

type DispatcherMiddleware[Req, Res any] func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res]

type HookDispatcher[Req, Res any] interface {
	OnCacheHit(ctx context.Context, req Req, res Res)
	OnCacheMiss(ctx context.Context, req Req)
	OnRetry(ctx context.Context, req Req, attempt int, err error)
	OnCoalesced(ctx context.Context, req Req)
	OnDeduplicated(ctx context.Context, req Req)
}

type Binding any

// DecodeFunc allows transports to inject data directly into the concrete type.
type DecodeFunc func(v any) error

// Executable handles the HOT path — execution only.
type Executable interface {
	ExecuteDecoded(ctx context.Context, decode DecodeFunc) (any, error)
}

// AnyDoer is the single-method interface for in-memory untyped execution.
type AnyDoer interface {
	DoAny(ctx context.Context, req any) (any, error)
}

// Describable handles the COLD path — boot, discovery, routing, and CLI help.
type Describable interface {
	Describe() *Meta
}

// AnyAction is the type-erased interface for the App and Transports to handle actions.
type AnyAction interface {
	Executable
	AnyDoer
	Describable
	GetBindings() []Binding
	GetAnyHooks() []AnyHook
	AddAnyHook(h ...AnyHook)
	CloneWithHooks(hooks ...AnyHook) AnyAction
}

// AnyStream is the type-erased stream returned at integration boundaries.
// Each yielded value is an item; a non-nil item error terminates the stream's
// current consumer according to the hosting Flow/transport policy.
type AnyStream func(yield func(item any, err error) bool)

// AnyStreamAction is the cold-path contract for stream actions. It is
// intentionally separate from AnyAction: a stream is lazy, can fail
// during iteration, and must not be coerced into a unary response.
//
// Hook lifecycle: Before fires when DoStreamAny is called; After and
// OnError fire when the stream is exhausted, errored, or the consumer
// stops early via yield(false). OnRetry/OnCacheHit/OnCacheMiss are
// reserved for future middleware and are not yet fired by StreamAction.
type AnyStreamAction interface {
	Describable
	TypedPayload
	GetBindings() []Binding
	GetAnyHooks() []AnyHook
	AddAnyHook(h ...AnyHook)
	CloneWithHooks(h ...AnyHook) AnyStreamAction
	DoStreamAny(ctx context.Context, req any) (AnyStream, error)
}

// TypedPayload allows plugins (like OpenAPI) to discover the underlying Request and Response
// types at boot time without storing them in metadata or using reflection during execution.
type TypedPayload interface {
	ReqPayload() any
	ResPayload() any
}
