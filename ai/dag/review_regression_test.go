package dag_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestReview_ResumedLoopCanRerunPersistedNode(t *testing.T) {
	var drafts atomic.Int32
	draft := action.New("draft", func(context.Context, dag.ReadState) (int, error) {
		return int(drafts.Add(1)) + 10, nil
	}).Build()
	review := action.New("review", func(context.Context, dag.ReadState) (int, error) { return 1, nil }).Build()
	graph, err := dag.New("loop").
		AddNode("draft", "", draft).AddNode("review", "", review).
		AddConditionalEdge("draft", func(context.Context, dag.ReadState) (string, error) { return "review", nil }).
		AddConditionalEdge("review", func(_ context.Context, state dag.ReadState) (string, error) {
			value, _ := state.Get(dag.OutputKey("draft"))
			draftValue, ok := value.(int)
			if !ok {
				return "", fmt.Errorf("draft output has type %T, want int", value)
			}
			if draftValue < 13 {
				return "draft", nil
			}
			return "end", nil
		}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	state := dag.AcquireState()
	defer state.Release()
	state.Set(dag.OutputKey("draft"), 11)
	if _, err := graph.Execute(context.Background(), state.AsRead()); err != nil {
		t.Fatalf("resumed loop failed (draft reran %d times): %v", drafts.Load(), err)
	}
}
