package dag

import (
	"context"

	"github.com/nexssp/kernel/xctx"
)

// LayerCallback is invoked by DAG.Execute after each layer completes.
//
// The state passed to the callback already includes every node output
// from the completed layer. A callback error aborts execution and is
// returned to the caller; the state at the point of failure is
// preserved, not released, so the caller can inspect or persist it.
//
// Nested DAGs do not fire the callback. The outermost DAG that finds
// a callback in its context consumes it, and every nested DAG in the
// same execution tree sees an empty slot. This makes the callback a
// checkpoint hook for one top-level execution, not for every graph in
// the tree.
type LayerCallback func(ctx context.Context, layer int, state *State) error

var (
	layerCallbackKey = xctx.NewKey[LayerCallback]("dag.layer_callback")
	consumedCbKey    = xctx.NewKey[struct{}]("dag.layer_callback_consumed")
)

// WithLayerCallback attaches a callback to ctx.
//
// Use it to checkpoint the DAG after every layer. A caller that keeps
// the last successful state, checks that the flow definition has not
// changed, and re-executes with that state gets resume-from-failure for
// free: DAG.Execute already skips any node whose output key is present
// in the initial state, so only the nodes that did not run are re-run.
func WithLayerCallback(ctx context.Context, fn LayerCallback) context.Context {
	if fn == nil {
		return ctx
	}
	return layerCallbackKey.With(ctx, fn)
}

// LayerCallbackFromCtx returns the callback attached by
// WithLayerCallback. It returns nil when no callback is set, and also
// when the callback has already been consumed by an outer DAG.Execute.
func LayerCallbackFromCtx(ctx context.Context) LayerCallback {
	if _, consumed := consumedCbKey.From(ctx); consumed {
		return nil
	}
	fn, _ := layerCallbackKey.From(ctx)
	return fn
}

// markCallbackConsumed returns a context that reports "already consumed"
// to any nested LayerCallbackFromCtx call.
func markCallbackConsumed(ctx context.Context) context.Context {
	return consumedCbKey.With(ctx, struct{}{})
}
