package main

import (
	"context"
	"fmt"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func main() {
	ctx := context.Background()

	// Sub-DAG: Pricing Calculation
	calcBase := action.New("sub.base", func(_ context.Context, _ *dag.NodeContext) (float64, error) {
		return 100.0, nil
	}).Build()

	applyTax := action.New("sub.tax", func(_ context.Context, n *dag.NodeContext) (float64, error) {
		base, _ := dag.GetNodeOutput[float64](n.Input, "sub_base")
		return base * 1.23, nil
	}).Build()

	subDAG, _ := dag.New("subgraph_pricing").
		AddNode("sub_base", "base", calcBase).
		AddNode("sub_tax", "tax", applyTax).
		AddEdge("sub_base", "sub_tax").
		Compile()

	// Master DAG: Embeds Sub-DAG as a single node
	auditAction := action.New("main.audit", func(_ context.Context, n *dag.NodeContext) (string, error) {
		subState, err := dag.GetNodeOutput[*dag.State](n.Input, "pricing_subgraph_node")
		if err != nil {
			return "", err
		}
		finalPrice, _ := dag.GetNodeOutput[float64](subState, "sub_tax")
		return fmt.Sprintf("Order audited. Gross price from sub-graph: %.2f USD", finalPrice), nil
	}).Build()

	masterDAG, err := dag.New("master_pipeline").
		AddNode("pricing_subgraph_node", "pricing_state", subDAG.AsAction().Build()).
		AddNode("audit_node", "audit_result", auditAction).
		AddEdge("pricing_subgraph_node", "audit_node").
		Compile()

	if err != nil {
		panic(err)
	}

	state := dag.AcquireState()
	defer state.Release()

	finalState, err := masterDAG.Execute(ctx, state)
	if err != nil {
		panic(err)
	}
	defer finalState.Release()

	report, _ := dag.GetNodeOutput[string](finalState, "audit_node")
	fmt.Println("🚀 Nested DAG Execution Result:")
	fmt.Println(report)
}
