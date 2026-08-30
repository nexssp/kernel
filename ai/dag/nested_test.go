package dag_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestNestedGraphAsNode(t *testing.T) {
	innerStep := action.New("inner.step", func(_ context.Context, input *dag.NodeContext) (string, error) {
		value, _ := input.Input.Get("seed")
		return value.(string) + ":inner", nil
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
		value, valueErr := dag.GetNodeOutput[string](state, "step")
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
	out, err := outer.Execute(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if value, err := dag.GetNodeOutput[string](out, "outer"); err != nil || value != "outer" {
		t.Fatalf("answer=%v err=%v", value, err)
	}
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
		value, valueErr := dag.GetNodeOutput[string](state, "step")
		if valueErr != nil {
			return "", valueErr
		}
		return value + ":final", nil
	}).Build()
	pipeline := action.Pipe("graph.pipeline", graphAction, final).Build()
	state := dag.AcquireState()
	defer state.Release()
	result, err := pipeline.Do(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if result != "graph:tasks.step.output:final" {
		t.Fatalf("result=%q", result)
	}
}
