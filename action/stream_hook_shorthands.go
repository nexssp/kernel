// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"iter"
)

// HookSuccessEvent runs fn when the stream completes successfully.
func (a *StreamAction[Req, T]) HookSuccessEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.Use(Hook[Req, iter.Seq2[T, error]]{
		After: func(_ context.Context, _ Req, _ iter.Seq2[T, error], err error, _ *Meta) {
			if err == nil {
				fn()
			}
		},
	})
}

// HookErrorEvent runs fn when the stream terminates with an error.
func (a *StreamAction[Req, T]) HookErrorEvent(fn func(error)) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.Use(Hook[Req, iter.Seq2[T, error]]{
		After: func(_ context.Context, _ Req, _ iter.Seq2[T, error], err error, _ *Meta) {
			if err != nil {
				fn(err)
			}
		},
	})
}

// HookCancelEvent runs fn when the stream context is canceled.
func (a *StreamAction[Req, T]) HookCancelEvent(fn func()) *StreamAction[Req, T] {
	if fn == nil {
		return a
	}
	return a.Use(Hook[Req, iter.Seq2[T, error]]{
		OnCancel: func(_ context.Context, _ Req, _ *Meta) { fn() },
	})
}

// AnyHook attaches a type-erased hook directly to the stream.
func (a *StreamAction[Req, T]) AnyHook(h ...AnyHook) *StreamAction[Req, T] {
	if a != nil {
		a.anyHooks = append(a.anyHooks, h...)
	}
	return a
}
