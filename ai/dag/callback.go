package dag

import (
	"context"

	"github.com/nexssp/kernel/xctx"
)

// LayerCallback is invoked by a layer-mode DAG's Execute after each layer
// completes. The state includes every node output from that layer. A callback
// error aborts execution; the state at failure is preserved for inspection or
// persistence.
//
// Nested DAGs do not invoke the callback independently. A dynamic outermost DAG
// consumes an attached callback without invoking it, so nested layer-mode DAGs
// do not fire it either.
type LayerCallback func(ctx context.Context, layer int, state *State) error

var (
	layerCallbackKey = xctx.NewKey[LayerCallback]("dag.layer_callback")
	consumedCbKey    = xctx.NewKey[struct{}]("dag.layer_callback_consumed")
)

// WithLayerCallback attaches a callback to ctx.
//
// Use it to checkpoint each layer of a layer-mode DAG. If the graph definition
// is unchanged, re-executing with the last successful state skips nodes whose
// outputs are already present, so only unfinished nodes run again.
func WithLayerCallback(ctx context.Context, fn LayerCallback) context.Context {
	if fn == nil {
		return ctx
	}
	return layerCallbackKey.With(ctx, fn)
}

// LayerCallbackFromCtx returns the callback attached by WithLayerCallback.
// It returns nil when no callback is set or an outer DAG has consumed it.
func LayerCallbackFromCtx(ctx context.Context) LayerCallback {
	if _, consumed := consumedCbKey.From(ctx); consumed {
		return nil
	}
	fn, _ := layerCallbackKey.From(ctx)
	return fn
}

// markCallbackConsumed returns a context that reports the callback consumed
// to any nested LayerCallbackFromCtx call.
func markCallbackConsumed(ctx context.Context) context.Context {
	return consumedCbKey.With(ctx, struct{}{})
}
