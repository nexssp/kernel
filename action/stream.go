package action

import (
	"context"
	"iter"

	"github.com/nexssp/kernel/xerr"
)

type StreamHandler[Req, T any] func(context.Context, Req) (iter.Seq2[T, error], error)

type StreamAction[Req, T any] struct {
	name    string
	handler StreamHandler[Req, T]
	hooks   []Hook[Req, iter.Seq2[T, error]]
}

func NewStream[Req, T any](name string, h StreamHandler[Req, T]) *StreamAction[Req, T] {
	return &StreamAction[Req, T]{name: name, handler: h}
}

func (a *StreamAction[Req, T]) Use(h ...Hook[Req, iter.Seq2[T, error]]) *StreamAction[Req, T] {
	a.hooks = append(a.hooks, h...)
	return a
}

func (a *StreamAction[Req, T]) Do(ctx context.Context, req Req) (seq iter.Seq2[T, error], err error) {
	meta := &Meta{Name: a.name}
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
