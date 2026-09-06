package action

import (
	"context"
	"iter"

	"github.com/nexssp/kernel/xerr"
)

// StreamHandler returns an iter.Seq2 that yields (item, error) pairs.
type StreamHandler[Req, T any] func(context.Context, Req) (iter.Seq2[T, error], error)

// StreamAction wraps a streaming handler with the standard lifecycle hooks.
type StreamAction[Req, T any] struct {
	name    string
	handler StreamHandler[Req, T]
	hooks   []Hook[Req, iter.Seq2[T, error]]
}

// NewStream creates a StreamAction.
func NewStream[Req, T any](name string, h StreamHandler[Req, T]) *StreamAction[Req, T] {
	return &StreamAction[Req, T]{name: name, handler: h}
}

// Use appends hooks to the stream lifecycle.
func (a *StreamAction[Req, T]) Use(h ...Hook[Req, iter.Seq2[T, error]]) *StreamAction[Req, T] {
	a.hooks = append(a.hooks, h...)
	return a
}

// Do initializes and wraps the iterator with full lifecycle hooks (Before/After/Panic).
func (a *StreamAction[Req, T]) Do(ctx context.Context, req Req) (iter.Seq2[T, error], error) {
	meta := &Meta{Name: a.name}
	var hooksRan int

	// 1. Run Before Hooks
	for _, h := range a.hooks {
		if h.Before != nil {
			var err error
			ctx, err = h.Before(ctx, req, meta)
			if err != nil {
				return func(yield func(T, error) bool) {
					var zero T
					yield(zero, err)
				}, err
			}
		}
		hooksRan++
	}

	// 2. Obtain the raw stream
	seq, err := a.handler(ctx, req)
	if err != nil {
		// Run After hooks on handler init failure
		for i := hooksRan - 1; i >= 0; i-- {
			if a.hooks[i].After != nil {
				a.hooks[i].After(ctx, req, nil, err, meta)
			}
		}
		return func(yield func(T, error) bool) {
			var zero T
			yield(zero, err)
		}, err
	}

	// 3. Wrap iterator so After hooks execute when the consumer finishes or breaks
	wrappedSeq := func(yield func(T, error) bool) {
		var lastErr error

		defer func() {
			if r := recover(); r != nil {
				lastErr = xerr.PanicRecovery(r)
			}
			// Run After hooks in LIFO order upon iterator termination
			for i := hooksRan - 1; i >= 0; i-- {
				if a.hooks[i].After != nil {
					a.hooks[i].After(ctx, req, seq, lastErr, meta)
				}
			}
		}()

		for item, itemErr := range seq {
			if itemErr != nil {
				lastErr = itemErr
			}
			if !yield(item, itemErr) {
				return // Consumer stopped early (e.g. break)
			}
		}
	}

	return wrappedSeq, nil
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
