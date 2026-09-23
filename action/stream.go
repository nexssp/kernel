package action

import (
	"context"
	"errors"
	"iter"
	"reflect"

	"github.com/nexssp/kernel/stream"
	"github.com/nexssp/kernel/xerr"
)

type StreamHandler[Req, T any] func(context.Context, Req) (iter.Seq2[T, error], error)

type StreamAction[Req, T any] struct {
	meta        *Meta
	bindings    []Binding
	handler     StreamHandler[Req, T]
	hooks       []Hook[Req, iter.Seq2[T, error]]
	anyHooks    []AnyHook
	streamHooks []StreamHook[Req, T]
}

func NewStream[Req, T any](name string, h StreamHandler[Req, T]) *StreamAction[Req, T] {
	return &StreamAction[Req, T]{meta: &Meta{Name: name}, handler: h}
}

var _ AnyStreamAction = (*StreamAction[struct{}, struct{}])(nil)

func (a *StreamAction[Req, T]) Describe() *Meta { return a.meta }

func (a *StreamAction[Req, T]) GetBindings() []Binding {
	return append([]Binding(nil), a.bindings...)
}

func (a *StreamAction[Req, T]) Route(bindings ...Binding) *StreamAction[Req, T] {
	a.bindings = append(a.bindings, bindings...)
	return a
}

func (a *StreamAction[Req, T]) ReqPayload() any {
	var req Req
	return req
}

func (a *StreamAction[Req, T]) ResPayload() any {
	var item T
	return item
}

// DoStreamAny is the dynamic integration boundary used by Flow and
// transports. When req's type already matches Req, the assertion is a
// single CPU cycle. Otherwise the request is coerced through the same
// JSON-tag-driven decoder that DoAny uses, so a map produced by the
// flow compiler (e.g. {dir: "/tmp/x", ext: "txt"}) lands in the typed
// Req struct instead of being silently dropped.
func (a *StreamAction[Req, T]) DoStreamAny(ctx context.Context, req any) (AnyStream, error) {
	typedReq, err := coerceStreamReq[Req](req)
	if err != nil {
		return nil, err
	}
	seq, err := a.Do(ctx, typedReq)
	if err != nil {
		return nil, err
	}
	return func(yield func(any, error) bool) {
		for item, itemErr := range seq {
			if !yield(item, itemErr) {
				return
			}
		}
	}, nil
}

// coerceStreamReq is the stream-boundary sibling of Coerce. It is
// identical in behavior but returns an explicit error instead of
// swallowing it, so a malformed request fails the stream source with
// a clear message instead of silently running with a zero value.
func coerceStreamReq[Req any](v any) (Req, error) {
	if t, ok := v.(Req); ok {
		return t, nil
	}
	var target Req
	if err := Assign(&target, v); err != nil {
		return target, err
	}
	return target, nil
}

func (a *StreamAction[Req, T]) Use(h ...Hook[Req, iter.Seq2[T, error]]) *StreamAction[Req, T] {
	a.hooks = append(a.hooks, h...)
	return a
}

// UseStream adds typed stream lifecycle hooks. OnItem is deliberately opt-in.
func (a *StreamAction[Req, T]) UseStream(h ...StreamHook[Req, T]) *StreamAction[Req, T] {
	a.streamHooks = append(a.streamHooks, h...)
	return a
}

func (a *StreamAction[Req, T]) GetAnyHooks() []AnyHook {
	if a == nil {
		return nil
	}
	out := make([]AnyHook, 0, len(a.hooks)+len(a.anyHooks))
	for _, h := range a.hooks {
		out = append(out, Adapt(h))
	}
	out = append(out, a.anyHooks...)
	return out
}

func (a *StreamAction[Req, T]) AddAnyHook(h ...AnyHook) {
	if a == nil || len(h) == 0 {
		return
	}
	reqType := reflect.TypeFor[Req]()
	resType := reflect.TypeFor[iter.Seq2[T, error]]()
	for _, hook := range h {
		if hook.OnBuild != nil && !hook.OnBuild(a.meta, reqType, resType) {
			continue
		}
		a.anyHooks = append(a.anyHooks, hook)
	}
}

func (a *StreamAction[Req, T]) CloneWithHooks(hooks ...AnyHook) AnyStreamAction {
	if a == nil {
		return nil
	}
	clone := &StreamAction[Req, T]{
		meta:        a.meta,
		bindings:    append([]Binding(nil), a.bindings...),
		handler:     a.handler,
		hooks:       append([]Hook[Req, iter.Seq2[T, error]](nil), a.hooks...),
		streamHooks: append([]StreamHook[Req, T](nil), a.streamHooks...),
	}
	clone.anyHooks = append(append([]AnyHook(nil), a.anyHooks...), hooks...)
	return clone
}

func (a *StreamAction[Req, T]) Do(ctx context.Context, req Req) (seq iter.Seq2[T, error], err error) {
	meta := a.meta
	var hooksRan, anyHooksRan int
	fireStreamStart(ctx, a.streamHooks, req, meta)

	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
			fireStreamTerminal(ctx, a.streamHooks, req, err, meta)
			fireStreamAfterHooks(ctx, a.hooks, a.anyHooks, hooksRan, anyHooksRan, req, nil, err, meta)
		}
	}()

	for i, h := range a.anyHooks {
		if h.Before != nil {
			var beforeErr error
			//nolint:fatcontext // bounded hook slice
			ctx, beforeErr = h.Before(ctx, any(req), meta)
			if beforeErr != nil {
				fireStreamTerminal(ctx, a.streamHooks, req, beforeErr, meta)
				fireStreamAfterHooks(ctx, a.hooks, a.anyHooks, hooksRan, i, req, nil, beforeErr, meta)
				return errorIterator[T](beforeErr), beforeErr
			}
		}
		anyHooksRan++
	}

	for i, h := range a.hooks {
		if h.Before != nil {
			var beforeErr error
			//nolint:fatcontext // bounded hook slice
			ctx, beforeErr = h.Before(ctx, req, meta)
			if beforeErr != nil {
				fireStreamTerminal(ctx, a.streamHooks, req, beforeErr, meta)
				fireStreamAfterHooks(ctx, a.hooks, a.anyHooks, i, anyHooksRan, req, nil, beforeErr, meta)
				return errorIterator[T](beforeErr), beforeErr
			}
		}
		hooksRan++
	}

	rawSeq, err := a.handler(ctx, req)
	if err != nil {
		fireStreamTerminal(ctx, a.streamHooks, req, err, meta)
		fireStreamAfterHooks(ctx, a.hooks, a.anyHooks, hooksRan, anyHooksRan, req, nil, err, meta)
		return errorIterator[T](err), err
	}

	wrappedSeq := func(yield func(T, error) bool) {
		var lastErr error
		defer func() {
			r := recover()
			if r != nil {
				lastErr = xerr.PanicRecovery(r)
			}
			fireStreamTerminal(ctx, a.streamHooks, req, lastErr, meta)
			fireStreamAfterHooks(ctx, a.hooks, a.anyHooks, hooksRan, anyHooksRan, req, rawSeq, lastErr, meta)
			if r != nil {
				panic(r)
			}
		}()

		for item, itemErr := range rawSeq {
			if itemErr != nil {
				lastErr = itemErr
			}
			if !yield(item, itemErr) {
				return
			}
			fireStreamItem(ctx, a.streamHooks, req, item, itemErr, meta)
		}
	}
	return wrappedSeq, nil
}

func errorIterator[T any](err error) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		yield(zero, err)
	}
}

func fireStreamAfterHooks[Req, T any](
	ctx context.Context,
	hooks []Hook[Req, iter.Seq2[T, error]],
	anyHooks []AnyHook,
	hooksRan, anyHooksRan int,
	req Req,
	seq iter.Seq2[T, error],
	err error,
	meta *Meta,
) {
	isTimeout := errors.Is(err, context.DeadlineExceeded)
	isCancel := errors.Is(err, context.Canceled)

	for i := anyHooksRan - 1; i >= 0; i-- {
		fireAnyStreamExitHook(ctx, anyHooks[i], req, seq, err, isTimeout, isCancel, meta)
	}
	for i := hooksRan - 1; i >= 0; i-- {
		fireTypedStreamExitHook(ctx, hooks[i], req, seq, err, isTimeout, isCancel, meta)
	}
}

func fireAnyStreamExitHook[Req, T any](
	ctx context.Context,
	h AnyHook,
	req Req,
	seq iter.Seq2[T, error],
	err error,
	isTimeout, isCancel bool,
	meta *Meta,
) {
	if isTimeout && h.OnTimeout != nil {
		callHook(meta, "OnTimeout", func() { h.OnTimeout(ctx, any(req), meta) })
	}
	if isCancel && h.OnCancel != nil {
		callHook(meta, "OnCancel", func() { h.OnCancel(ctx, any(req), meta) })
	}
	if err != nil && h.OnError != nil {
		callHook(meta, "OnError", func() { h.OnError(ctx, any(req), err, meta) })
	}
	if err == nil && h.OnSuccess != nil {
		callHook(meta, "OnSuccess", func() { h.OnSuccess(ctx, any(req), any(seq), meta) })
	}
	if h.After != nil {
		callHook(meta, "After", func() { h.After(ctx, any(req), any(seq), err, meta) })
	}
}

func fireTypedStreamExitHook[Req, T any](
	ctx context.Context,
	h Hook[Req, iter.Seq2[T, error]],
	req Req,
	seq iter.Seq2[T, error],
	err error,
	isTimeout, isCancel bool,
	meta *Meta,
) {
	if isTimeout && h.OnTimeout != nil {
		callHook(meta, "OnTimeout", func() { h.OnTimeout(ctx, req, meta) })
	}
	if isCancel && h.OnCancel != nil {
		callHook(meta, "OnCancel", func() { h.OnCancel(ctx, req, meta) })
	}
	if err != nil && h.OnError != nil {
		callHook(meta, "OnError", func() { h.OnError(ctx, req, err, meta) })
	}
	if err == nil && h.OnSuccess != nil {
		callHook(meta, "OnSuccess", func() { h.OnSuccess(ctx, req, seq, meta) })
	}
	if h.After != nil {
		callHook(meta, "After", func() { h.After(ctx, req, seq, err, meta) })
	}
}

func CollectStream[Req, T any](ctx context.Context, a *StreamAction[Req, T], req Req) (out []T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
		}
	}()
	seq, err := a.Do(ctx, req)
	if err != nil {
		return out, err
	}
	for item, itemErr := range seq {
		if itemErr != nil {
			return out, itemErr
		}
		out = append(out, item)
	}
	return out, nil
}

func NewStreamOp[Req, In, Out any](name string, source *StreamAction[Req, In], op stream.StreamOp[In, Out]) *StreamAction[Req, Out] {
	return NewStream(name, func(ctx context.Context, req Req) (iter.Seq2[Out, error], error) {
		up, err := source.Do(ctx, req)
		if err != nil {
			return nil, err
		}
		return op(up), nil
	})
}
