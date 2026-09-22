package action

import (
	"context"
	"iter"

	"github.com/nexssp/kernel/xerr"
)

type StreamHandler[Req, T any] func(context.Context, Req) (iter.Seq2[T, error], error)

type StreamAction[Req, T any] struct {
	meta     *Meta
	bindings []Binding
	handler  StreamHandler[Req, T]
	hooks    []Hook[Req, iter.Seq2[T, error]]
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
// transports. A mismatched request is converted to the zero value rather
// than panicking, matching the kernel's other dynamic boundaries.
func (a *StreamAction[Req, T]) DoStreamAny(ctx context.Context, req any) (AnyStream, error) {
	seq, err := a.Do(ctx, assertTo[Req](req))
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

func (a *StreamAction[Req, T]) Use(h ...Hook[Req, iter.Seq2[T, error]]) *StreamAction[Req, T] {
	a.hooks = append(a.hooks, h...)
	return a
}

func (a *StreamAction[Req, T]) Do(ctx context.Context, req Req) (seq iter.Seq2[T, error], err error) {
	meta := a.meta
	var hooksRan int

	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
			fireStreamAfterHooks(ctx, a.hooks, hooksRan, req, nil, err, meta)
		}
	}()

	for _, h := range a.hooks {
		if h.Before != nil {
			var beforeErr error
			//nolint:fatcontext // bounded hook slice requires sequential context propagation
			ctx, beforeErr = h.Before(ctx, req, meta)
			if beforeErr != nil {
				return func(yield func(T, error) bool) {
					var zero T
					yield(zero, beforeErr)
				}, beforeErr
			}
		}
		hooksRan++
	}

	rawSeq, err := a.handler(ctx, req)
	if err != nil {
		fireStreamAfterHooks(ctx, a.hooks, hooksRan, req, nil, err, meta)
		return func(yield func(T, error) bool) {
			var zero T
			yield(zero, err)
		}, err
	}

	wrappedSeq := func(yield func(T, error) bool) {
		var lastErr error

		defer func() {
			r := recover()
			if r != nil {
				lastErr = xerr.PanicRecovery(r)
			}
			fireStreamAfterHooks(ctx, a.hooks, hooksRan, req, rawSeq, lastErr, meta)
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
		}
	}

	return wrappedSeq, nil
}

func fireStreamAfterHooks[Req, T any](
	ctx context.Context,
	hooks []Hook[Req, iter.Seq2[T, error]],
	hooksRan int,
	req Req,
	seq iter.Seq2[T, error],
	err error,
	meta *Meta,
) {
	for i := hooksRan - 1; i >= 0; i-- {
		if hooks[i].After != nil {
			callHook(meta, "After", func() {
				hooks[i].After(ctx, req, seq, err, meta)
			})
		}
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
