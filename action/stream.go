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
	anyHooks []AnyHook
}

func NewStream[Req, T any](name string, h StreamHandler[Req, T]) *StreamAction[Req, T] {
	return &StreamAction[Req, T]{meta: &Meta{Name: name}, handler: h}
}

var _ AnyStreamAction = (*StreamAction[struct{}, struct{}])(nil)

// ── Metadata ────────────────────────────────────────────────────────────

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

// ── Hook registration ───────────────────────────────────────────────────

// Use attaches typed hooks. The typed hook signature differs from
// AnyAction: the "response" side is the iterator itself, not a single
// value. Lifecycle events fire when the stream is exhausted, errored,
// or the consumer stops early via yield(false).
func (a *StreamAction[Req, T]) Use(h ...Hook[Req, iter.Seq2[T, error]]) *StreamAction[Req, T] {
	a.hooks = append(a.hooks, h...)
	return a
}

// GetAnyHooks returns a snapshot of both typed and erased hooks, with
// typed hooks adapted through Adapt so the caller sees a uniform slice.
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

// AddAnyHook appends erased hooks. They fire with the stream's input
// request and the resulting iterator as the "response".
func (a *StreamAction[Req, T]) AddAnyHook(h ...AnyHook) {
	if a == nil || len(h) == 0 {
		return
	}
	a.anyHooks = append(a.anyHooks, h...)
}

// CloneWithHooks returns a new StreamAction carrying the same handler,
// bindings, and existing hooks, plus the ones passed in. The receiver
// is never modified. Used by registries to apply library-level hooks
// without mutating originals.
func (a *StreamAction[Req, T]) CloneWithHooks(hooks ...AnyHook) AnyStreamAction {
	if a == nil {
		return nil
	}
	clone := &StreamAction[Req, T]{
		meta:     a.meta,
		bindings: append([]Binding(nil), a.bindings...),
		handler:  a.handler,
		hooks:    append([]Hook[Req, iter.Seq2[T, error]](nil), a.hooks...),
	}
	clone.anyHooks = append(clone.anyHooks, a.anyHooks...)
	clone.anyHooks = append(clone.anyHooks, hooks...)
	return clone
}

// ── Execution ───────────────────────────────────────────────────────────

// DoStreamAny is the dynamic integration boundary used by Flow and
// transports. A mismatched request is converted to the zero value
// rather than panicking, matching the kernel's other dynamic boundaries.
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

func (a *StreamAction[Req, T]) Do(ctx context.Context, req Req) (seq iter.Seq2[T, error], err error) {
	meta := a.meta
	var typedHooksRan, anyHooksRan int

	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
			fireStreamAfterHooks(ctx, a.hooks, typedHooksRan, req, nil, err, meta)
			fireStreamAnyAfterHooks(ctx, a.anyHooks, anyHooksRan, req, nil, err, meta)
		}
	}()

	// ── Before: anyHooks first (outermost layer), then typed hooks ──
	var beforeErr error

	ctx, anyHooksRan, beforeErr = runStreamAnyBeforeHooks(ctx, a.anyHooks, req, meta)
	if beforeErr != nil {
		return func(yield func(T, error) bool) {
			var zero T
			yield(zero, beforeErr)
		}, beforeErr
	}

	ctx, typedHooksRan, beforeErr = runStreamTypedBeforeHooks(ctx, a.hooks, req, meta)
	if beforeErr != nil {
		return func(yield func(T, error) bool) {
			var zero T
			yield(zero, beforeErr)
		}, beforeErr
	}

	rawSeq, err := a.handler(ctx, req)
	if err != nil {
		fireStreamAfterHooks(ctx, a.hooks, typedHooksRan, req, nil, err, meta)
		fireStreamAnyAfterHooks(ctx, a.anyHooks, anyHooksRan, req, nil, err, meta)
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
			fireStreamAfterHooks(ctx, a.hooks, typedHooksRan, req, rawSeq, lastErr, meta)
			fireStreamAnyAfterHooks(ctx, a.anyHooks, anyHooksRan, req, rawSeq, lastErr, meta)
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

// ── Before helpers ──────────────────────────────────────────────────────

func runStreamTypedBeforeHooks[Req, T any](
	ctx context.Context,
	hooks []Hook[Req, iter.Seq2[T, error]],
	req Req,
	meta *Meta,
) (context.Context, int, error) {
	for i, h := range hooks {
		if h.Before == nil {
			continue
		}
		var err error
		//nolint:fatcontext // bounded hook slice requires sequential context propagation
		ctx, err = h.Before(ctx, req, meta)
		if err != nil {
			return ctx, i, err
		}
	}
	return ctx, len(hooks), nil
}

// runStreamAnyBeforeHooks takes only Req: T is not used anywhere in
// the body, so the compiler cannot infer it from the call site.
func runStreamAnyBeforeHooks[Req any](
	ctx context.Context,
	hooks []AnyHook,
	req Req,
	meta *Meta,
) (context.Context, int, error) {
	for i, h := range hooks {
		if h.Before == nil {
			continue
		}
		var err error
		//nolint:fatcontext // bounded hook slice requires sequential context propagation
		ctx, err = h.Before(ctx, any(req), meta)
		if err != nil {
			return ctx, i, err
		}
	}
	return ctx, len(hooks), nil
}

// ── After helpers ───────────────────────────────────────────────────────

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

// fireStreamAnyAfterHooks takes [Req any] and seq as `any`. T was
// dropped because the compiler cannot infer it from call sites where
// seq is nil (panic-path and handler-error-path), and the hook
// signature already accepts `any` for both request and response.
func fireStreamAnyAfterHooks[Req any](
	ctx context.Context,
	hooks []AnyHook,
	hooksRan int,
	req Req,
	seq any,
	err error,
	meta *Meta,
) {
	for i := hooksRan - 1; i >= 0; i-- {
		h := hooks[i]
		if h.After != nil {
			callHook(meta, "After", func() {
				h.After(ctx, any(req), seq, err, meta)
			})
		}
	}
}

// ── Convenience ─────────────────────────────────────────────────────────

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
