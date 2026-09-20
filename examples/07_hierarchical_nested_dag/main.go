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
		base, err := dag.GetNodeOutput[float64](n.Input, "sub_base")
		if err != nil {
			return 0, err
		}
		return base * 1.23, nil
	}).Build()

	subDAG, err := dag.New("subgraph_pricing").
		AddNode("sub_base", "base", calcBase).
		AddNode("sub_tax", "tax", applyTax).
		AddEdge("sub_base", "sub_tax").
		Compile()
	if err != nil {
		panic(err)
	}

	auditAction := action.New("main.audit", func(_ context.Context, n *dag.NodeContext) (string, error) {
		subState, subErr := dag.GetNodeOutput[*dag.State](n.Input, "pricing_subgraph_node")
		if subErr != nil {
			return "", subErr
		}
		finalPrice, priceErr := dag.GetNodeOutput[float64](subState.AsRead(), "sub_tax")
		if priceErr != nil {
			return "", priceErr
		}
		return fmt.Sprintf("Order audited. Gross price from sub-graph: %.2f USD", finalPrice), nil
	}).Build()

	masterDAG, compileErr := dag.New("master_pipeline").
		AddNode("pricing_subgraph_node", "pricing_state", subDAG.AsAction().Build()).
		AddNode("audit_node", "audit_result", auditAction).
		AddEdge("pricing_subgraph_node", "audit_node").
		Compile()
	if compileErr != nil {
		panic(compileErr)
	}

	state := dag.AcquireState()
	defer state.Release()

	finalState, execErr := masterDAG.Execute(ctx, state.AsRead())
	if finalState != nil {
		defer finalState.Release()
	}
	if execErr != nil {
		panic(execErr)
	}

	report, reportErr := dag.GetNodeOutput[string](finalState.AsRead(), "audit_node")
	if reportErr != nil {
		panic(reportErr)
	}
	fmt.Println("🚀 Nested DAG Execution Result:")
	fmt.Println(report)
}
