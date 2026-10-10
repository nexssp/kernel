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
			callHook3(meta, "Stream.OnStart", h.OnStart, ctx, req, meta)
		}
	}
}

func fireStreamItem[Req, Item any](ctx context.Context, hooks []StreamHook[Req, Item], req Req, item Item, itemErr error, meta *Meta) {
	for i := range hooks {
		if hooks[i].OnItem != nil {
			h := hooks[i]
			callHook5(meta, "Stream.OnItem", h.OnItem, ctx, req, item, itemErr, meta)
		}
	}
}

func fireStreamTerminal[Req, Item any](ctx context.Context, hooks []StreamHook[Req, Item], req Req, err error, meta *Meta) {
	isTimeout := errors.Is(err, context.DeadlineExceeded)
	isCancel := errors.Is(err, context.Canceled)
	for _, h := range slices.Backward(hooks) {
		if isTimeout && h.OnTimeout != nil {
			callHook3(meta, "Stream.OnTimeout", h.OnTimeout, ctx, req, meta)
		}
		if isCancel && h.OnCancel != nil {
			callHook3(meta, "Stream.OnCancel", h.OnCancel, ctx, req, meta)
		}
		if err != nil && h.OnError != nil {
			callHook4(meta, "Stream.OnError", h.OnError, ctx, req, err, meta)
		}
		if err == nil && h.OnSuccess != nil {
			callHook3(meta, "Stream.OnSuccess", h.OnSuccess, ctx, req, meta)
		}
		if h.After != nil {
			callHook4(meta, "Stream.After", h.After, ctx, req, err, meta)
		}
	}
}
