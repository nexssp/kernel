// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
)

// HookStartEvent runs fn when the stream execution begins.
func (a *StreamAction[Req, T]) HookStartEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.UseStream(StreamHook[Req, T]{
		OnStart: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookSuccessEvent runs fn when the stream completes successfully (EOF).
func (a *StreamAction[Req, T]) HookSuccessEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.UseStream(StreamHook[Req, T]{
		OnSuccess: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookErrorEvent runs fn when the stream terminates with an error.
func (a *StreamAction[Req, T]) HookErrorEvent(fn func(error)) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.UseStream(StreamHook[Req, T]{
		OnError: func(_ context.Context, _ Req, err error, _ *Meta) { fn(err) },
	})
}

// HookTimeoutEvent runs fn when the stream context exceeds its deadline.
func (a *StreamAction[Req, T]) HookTimeoutEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.UseStream(StreamHook[Req, T]{
		OnTimeout: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookCancelEvent runs fn when the stream context is explicitly canceled.
func (a *StreamAction[Req, T]) HookCancelEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.UseStream(StreamHook[Req, T]{
		OnCancel: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// HookAfterEvent runs fn unconditionally when the stream is fully closed.
func (a *StreamAction[Req, T]) HookAfterEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.UseStream(StreamHook[Req, T]{
		After: func(_ context.Context, _ Req, _ error, _ *Meta) { fn() },
	})
}

// AnyHook attaches a type-erased hook directly to the stream.
func (a *StreamAction[Req, T]) AnyHook(h ...AnyHook) *StreamAction[Req, T] {
	if a != nil {
		a.anyHooks = append(a.anyHooks, h...)
	}
	return a
}
