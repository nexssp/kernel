package dag_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestDAG_FailedLayerWithDetachedReader(t *testing.T) {
	t.Parallel()
	var captured dag.ReadState
	var wg sync.WaitGroup

	spawner := action.New("spawner", func(_ context.Context, n *dag.NodeContext) (string, error) {
		captured = n.Input
		wg.Go(func() {
			time.Sleep(30 * time.Millisecond)
			_, _ = captured.Get("seed") // czyta mapę, którą handleLayerError może zamutować
		})
		return "ok", nil
	}).Build()

	failer := action.New("failer", func(_ context.Context, _ *dag.NodeContext) (string, error) {
		return "", errors.New("boom")
	}).Build()

	g, _ := dag.New("race_layer").
		AddNode("a", "a", spawner).
		AddNode("b", "b", failer).
		Compile()

	state := dag.AcquireState()
	state.Set("seed", "payload")
	defer state.Release()

	final, _ := g.Execute(context.Background(), state.AsRead())
	if final != nil {
		final.Release()
	}
	wg.Wait()
}
