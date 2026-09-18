package dag_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

// TestDAG_DetachedReadSurvivesRelease proves the headline guarantee of
// the ReadState design: a goroutine that captures NodeContext.Input may
// read it after the graph has finished and after every *State has been
// Released. The old pool-based implementation corrupted this read; the
// current implementation hands back a frozen snapshot whose backing map
// is kept alive by the ReadState reference alone.
func TestDAG_DetachedReadSurvivesRelease(t *testing.T) {
	var (
		captured dag.ReadState
		wg       sync.WaitGroup
	)

	spawner := action.New("spawner", func(_ context.Context, n *dag.NodeContext) (string, error) {
		captured = n.Input // capture the read view
		wg.Go(func() {
			time.Sleep(20 * time.Millisecond)
			if v, ok := captured.Get("seed"); !ok || v != "payload" {
				t.Errorf("detached read got %v, ok=%v; want payload, true", v, ok)
			}
		})
		return "s", nil
	}).Build()

	g, err := dag.New("detached_read").AddNode("s", "s", spawner).Compile()
	if err != nil {
		t.Fatal(err)
	}

	state := dag.AcquireState()
	state.Set("seed", "payload")

	out, err := g.Execute(context.Background(), state.AsRead())
	state.Release()
	if out != nil {
		out.Release()
	}
	if err != nil {
		t.Fatal(err)
	}

	// Churn fresh states with colliding keys. Under the old pool design
	// AcquireState would hand back the reader's map and these Sets would
	// silently corrupt the detached read.
	for i := range 32 {
		s := dag.AcquireState()
		s.Set("seed", fmt.Sprintf("overwritten-%d", i))
		s.Set("junk", "junk")
		s.Release()
	}

	wg.Wait()
}
