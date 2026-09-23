package action

import (
	"context"
	"errors"
	"slices"
)

// StreamHook observes one typed stream execution. Lifecycle callbacks run at
// most once per execution. OnItem is opt-in and runs once per yielded item.
type StreamHook[Req, Item any] struct {
	OnStart   func(context.Context, Req, *Meta)
	OnItem    func(context.Context, Req, Item, error, *Meta)
	OnSuccess func(context.Context, Req, *Meta)
	OnTimeout func(context.Context, Req, *Meta)
	OnCancel  func(context.Context, Req, *Meta)
	OnError   func(context.Context, Req, error, *Meta)
	After     func(context.Context, Req, error, *Meta)
}

func fireStreamStart[Req, Item any](ctx context.Context, hooks []StreamHook[Req, Item], req Req, meta *Meta) {
	for i := range hooks {
		if hooks[i].OnStart != nil {
			h := hooks[i]
			callHook(meta, "Stream.OnStart", func() { h.OnStart(ctx, req, meta) })
		}
	}
}

func fireStreamItem[Req, Item any](ctx context.Context, hooks []StreamHook[Req, Item], req Req, item Item, itemErr error, meta *Meta) {
	for i := range hooks {
		if hooks[i].OnItem != nil {
			h := hooks[i]
			callHook(meta, "Stream.OnItem", func() { h.OnItem(ctx, req, item, itemErr, meta) })
		}
	}
}

func fireStreamTerminal[Req, Item any](ctx context.Context, hooks []StreamHook[Req, Item], req Req, err error, meta *Meta) {
	isTimeout := errors.Is(err, context.DeadlineExceeded)
	isCancel := errors.Is(err, context.Canceled)
	for _, h := range slices.Backward(hooks) {
		if isTimeout && h.OnTimeout != nil {
			callHook(meta, "Stream.OnTimeout", func() { h.OnTimeout(ctx, req, meta) })
		}
		if isCancel && h.OnCancel != nil {
			callHook(meta, "Stream.OnCancel", func() { h.OnCancel(ctx, req, meta) })
		}
		if err != nil && h.OnError != nil {
			callHook(meta, "Stream.OnError", func() { h.OnError(ctx, req, err, meta) })
		}
		if err == nil && h.OnSuccess != nil {
			callHook(meta, "Stream.OnSuccess", func() { h.OnSuccess(ctx, req, meta) })
		}
		if h.After != nil {
			callHook(meta, "Stream.After", func() { h.After(ctx, req, err, meta) })
		}
	}
}
