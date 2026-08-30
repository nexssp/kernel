package action

import (
	"context"
	"iter"
)

// StreamHandler returns an iter.Seq2 that yields (item, error) pairs.
type StreamHandler[Req, T any] func(context.Context, Req) (iter.Seq2[T, error], error)

// StreamAction wraps a streaming handler with the standard middleware chain.
type StreamAction[Req, T any] struct {
	name    string
	handler StreamHandler[Req, T]
	hooks   []Hook[Req, iter.Seq2[T, error]]
}

// NewStream creates a StreamAction.
func NewStream[Req, T any](name string, h StreamHandler[Req, T]) *StreamAction[Req, T] {
	return &StreamAction[Req, T]{name: name, handler: h}
}

// Use appends middleware to the setup phase.
func (a *StreamAction[Req, T]) Use(h ...Hook[Req, iter.Seq2[T, error]]) *StreamAction[Req, T] {
	a.hooks = append(a.hooks, h...)
	return a
}

// Do returns the iterator.
func (a *StreamAction[Req, T]) Do(ctx context.Context, req Req) (iter.Seq2[T, error], error) {
	var err error
	// Run before hooks
	for _, h := range a.hooks {
		if h.Before != nil {
			ctx, err = h.Before(ctx, req, &Meta{Name: a.name}) //nolint:fatcontext // context is intentionally chained
			if err != nil {
				return func(yield func(T, error) bool) {
					yield(*new(T), err)
				}, err
			}
		}
	}

	seq, err := a.handler(ctx, req)
	if err != nil {
		return func(yield func(T, error) bool) {
			yield(*new(T), err)
		}, err
	}

	return seq, nil
}

// CollectStream runs the entire stream and returns all items as a slice.
func CollectStream[Req, T any](ctx context.Context, a *StreamAction[Req, T], req Req) ([]T, error) {
	var out []T
	seq, err := a.Do(ctx, req)
	if err != nil {
		return out, err
	}
	for item, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, item)
	}
	return out, nil
}
