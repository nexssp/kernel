package dag

import (
	"context"
	"errors"

	"github.com/nexssp/kernel/action"
	"golang.org/x/sync/errgroup"
)

// Execute runs DAG layers sequentially, executing nodes within each layer
// concurrently.
//
// Resume-from-failure: any node whose output key is already present in
// initialState is skipped. Re-execute with a persisted state to run only
// the nodes that did not complete.
//
// Layer callback: attach with WithLayerCallback. Fired after each layer
// with the accumulated state. A callback error aborts execution and is
// returned wrapped in *ExecutionError, unless the callback returned
// ErrSuspended (preserved as the sentinel). The callback is consumed by
// the outermost DAG that sees it, so nested DAGs never fire it twice.
//
// On any error the returned *State is non-nil and owns every node output
// produced so far. The caller must Release it.
func (d *DAG) Execute(ctx context.Context, initialState ReadState) (*State, error) {
	currentState := initialState.Clone()
	onLayer := LayerCallbackFromCtx(ctx)
	if onLayer != nil {
		ctx = markCallbackConsumed(ctx)
	}

	for layerIdx, layer := range d.layers {
		layerResults, err := runLayer(ctx, layer, currentState)
		if err != nil {
			return handleLayerError(err, currentState, layerResults, layer, layerIdx)
		}

		currentState = mergeLayerResults(currentState, layerResults)

		if onLayer != nil {
			if cbErr := onLayer(ctx, layerIdx, currentState); cbErr != nil {
				return handleCallbackError(cbErr, currentState, layerIdx)
			}
		}
	}

	return currentState, nil
}

func runLayer(ctx context.Context, layer []Node, currentState *State) ([]Value, error) {
	g, layerCtx := errgroup.WithContext(ctx)
	results := make([]Value, len(layer))

	for i, node := range layer {
		idx, n := i, node

		if existingVal, ok := currentState.Get(n.OutputKey); ok {
			results[idx] = Value{Key: n.OutputKey, Val: existingVal}
			continue
		}

		g.Go(func() error {
			return executeNode(layerCtx, n, currentState, &results[idx])
		})
	}

	return results, g.Wait()
}

func executeNode(ctx context.Context, n Node, currentState *State, out *Value) error {
	nCtx := &NodeContext{Input: currentState.AsRead(), Key: n.OutputKey}

	outVal, err := runNodeAction(ctx, n, currentState, nCtx, out)
	if err != nil {
		return err
	}
	*out = Value{Key: n.OutputKey, Val: outVal}
	return nil
}

// runNodeAction dispatches to the nested-DAG fast path or the general
// DecodeFunc path.
//
// On nested failure, runNestedDAG writes the partial inner *State into
// out (under InnerStateKey) before returning. handleLayerError then
// merges it into the state it hands back, so resume callers can re-run
// the nested graph from where it stopped.
func runNodeAction(
	ctx context.Context,
	n Node,
	currentState *State,
	nCtx *NodeContext,
	out *Value,
) (any, error) {
	if nested, ok := n.Action.(*action.BuiltAction[ReadState, *State]); ok {
		return runNestedDAG(ctx, n, currentState, nCtx, nested, out)
	}

	outVal, err := n.Action.ExecuteDecoded(ctx, nodeContextDecoder(nCtx))
	if err != nil {
		if errors.Is(err, ErrSuspended) {
			return nil, err
		}
		return nil, &ExecutionError{FailedNode: n.ID, Cause: err}
	}
	return outVal, nil
}

// runNestedDAG invokes an inner DAG as a single node. It forwards any
// inner state saved from a previous run (resume), and on failure it
// stashes the current inner state into out so the parent can persist it.
func runNestedDAG(
	ctx context.Context,
	n Node,
	currentState *State,
	nCtx *NodeContext,
	nested *action.BuiltAction[ReadState, *State],
	out *Value,
) (any, error) {
	input := nCtx.Input
	if saved, ok := currentState.Get(InnerStateKey(n.ID)); ok {
		if st, isState := saved.(*State); isState && st != nil {
			input = st.AsRead()
		}
	}

	outVal, err := nested.Do(ctx, input)
	if err == nil {
		return outVal, nil
	}

	if outVal != nil {
		*out = Value{Key: InnerStateKey(n.ID), Val: outVal}
	}
	if errors.Is(err, ErrSuspended) {
		return nil, err
	}
	return nil, &ExecutionError{FailedNode: n.ID, Cause: err}
}

func nodeContextDecoder(nCtx *NodeContext) action.DecodeFunc {
	return func(v any) error {
		switch target := v.(type) {
		case *NodeContext:
			*target = *nCtx
			return nil
		case **NodeContext:
			*target = nCtx
			return nil
		}
		return nil
	}
}

func mergeLayerResults(currentState *State, layerResults []Value) *State {
	nextState := currentState.Clone()
	currentState.Release()
	applyResults(nextState, layerResults)
	return nextState
}

func handleLayerError(
	err error,
	currentState *State,
	layerResults []Value,
	layer []Node,
	layerIdx int,
) (*State, error) {
	finalState := currentState.Clone()
	currentState.Release()
	applyResults(finalState, layerResults)

	if errors.Is(err, ErrSuspended) {
		return finalState, err
	}

	if execErr, ok := errors.AsType[*ExecutionError](err); ok {
		execErr.Layer = layerIdx
		execErr.State = finalState
		for i, res := range layerResults {
			if res.Key != "" && res.Val != nil {
				execErr.Completed = append(execErr.Completed, layer[i].ID)
			}
		}
		return finalState, execErr
	}
	return finalState, err
}

// applyResults folds layer outputs into dst, skipping empty keys and nil
// values. Shared by the merge and error paths so both accumulate node
// outputs with identical rules.
func applyResults(dst *State, results []Value) {
	for _, res := range results {
		if res.Key != "" && res.Val != nil {
			dst.data[res.Key] = res.Val
		}
	}
}

func handleCallbackError(cbErr error, currentState *State, layerIdx int) (*State, error) {
	if errors.Is(cbErr, ErrSuspended) {
		return currentState, cbErr
	}
	return currentState, &ExecutionError{
		Layer:      layerIdx,
		FailedNode: "",
		Cause:      cbErr,
		State:      currentState,
	}
}
