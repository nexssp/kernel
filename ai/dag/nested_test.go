package dag_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestNestedGraphAsNode(t *testing.T) {
	innerStep := action.New("inner.step", func(_ context.Context, input *dag.NodeContext) (string, error) {
		value, _ := input.Input.Get("seed")
		s, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("seed is not a string: %T", value)
		}
		return s + ":inner", nil
	}).Build()
	inner, err := dag.New("inner").AddNode("step", "inner_value", innerStep).Compile()
	if err != nil {
		t.Fatal(err)
	}
	innerAction := inner.AsAction().Build()

	outerStep := action.New("outer.step", func(_ context.Context, input *dag.NodeContext) (string, error) {
		state, stateErr := dag.GetNodeOutput[*dag.State](input.Input, "nested")
		if stateErr != nil {
			t.Fatalf("nested graph result missing: %v", stateErr)
		}
		value, valueErr := dag.GetNodeOutput[string](state.AsRead(), "step")
		if valueErr != nil || value != "seed:inner" {
			t.Fatalf("nested state=%v err=%v", state.Data(), valueErr)
		}
		return "outer", nil
	}).Build()
	outer, err := dag.New("outer").
		AddNode("nested", "nested", innerAction).
		AddNode("outer", "answer", outerStep).
		AddEdge("nested", "outer").
		Compile()
	if err != nil {
		t.Fatal(err)
	}

	state := dag.AcquireState()
	state.Set("seed", "seed")
	defer state.Release()
	out, err := outer.Execute(context.Background(), state.AsRead())
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if value, err := dag.GetNodeOutput[string](out.AsRead(), "outer"); err != nil || value != "outer" {
		t.Fatalf("answer=%v err=%v", value, err)
	}
}

func TestNestedDAG_InnerErrorPreservesResumeProgress(t *testing.T) {
	var step1Runs, step2Runs int
	inner1 := action.New("inner1", func(context.Context, *dag.NodeContext) (string, error) {
		step1Runs++
		return "ok1", nil
	}).Build()
	inner2 := action.New("inner2", func(context.Context, *dag.NodeContext) (string, error) {
		step2Runs++
		return "", errors.New("boom")
	}).Build()

	subDAG, _ := dag.New("sub").
		AddNode("i1", "i1", inner1).
		AddNode("i2", "i2", inner2).
		AddEdge("i1", "i2").
		Compile()

	outer, _ := dag.New("outer").
		AddNode("sub_node", "sub_out", subDAG.AsAction().Build()).
		Compile()

	state := dag.AcquireState()

	final1, err1 := outer.Execute(context.Background(), state.AsRead())
	if err1 == nil {
		t.Fatal("expected error")
	}

	if step1Runs != 1 || step2Runs != 1 {
		t.Fatalf("expected 1 run each, got %d and %d", step1Runs, step2Runs)
	}

	// Resume
	final2, err2 := outer.Execute(context.Background(), final1.AsRead())
	if err2 == nil {
		t.Fatal("expected error again")
	}

	if step1Runs != 1 {
		t.Fatalf("inner1 should NOT run again on resume! Runs: %d", step1Runs)
	}
	if step2Runs != 2 {
		t.Fatalf("inner2 should run again. Runs: %d", step2Runs)
	}

	state.Release()
	final1.Release()
	final2.Release()
}

func TestGraphActionPipesWithAction(t *testing.T) {
	step := action.New("step", func(_ context.Context, input *dag.NodeContext) (string, error) {
		return "graph:" + input.Key, nil
	}).Build()
	graph, err := dag.New("graph").AddNode("step", "value", step).Compile()
	if err != nil {
		t.Fatal(err)
	}
	graphAction := graph.AsAction().Build()
	final := action.New("final", func(_ context.Context, state *dag.State) (string, error) {
		value, valueErr := dag.GetNodeOutput[string](state.AsRead(), "step")
		if valueErr != nil {
			return "", valueErr
		}
		return value + ":final", nil
	}).Build()
	pipeline := action.Pipe("graph.pipeline", graphAction, final).Build()
	state := dag.AcquireState()
	defer state.Release()
	result, err := pipeline.Do(context.Background(), state.AsRead())
	if err != nil {
		t.Fatal(err)
	}
	if result != "graph:tasks.step.output:final" {
		t.Fatalf("result=%q", result)
	}
}
