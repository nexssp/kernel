package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

type UserProfile struct {
	ID   string
	Name string
	Tier string
}

type OrderSummary struct {
	TotalSpent float64
	Count      int
}

func main() {
	ctx := context.Background()

	// 1. Layer 0 (Parallel Node A): Fetch User Profile
	fetchUser := action.New("user.fetch", func(ctx context.Context, nCtx *dag.NodeContext) (UserProfile, error) {
		time.Sleep(20 * time.Millisecond) // Simulated network latency
		return UserProfile{ID: "usr_42", Name: "Alice", Tier: "VIP"}, nil
	}).Build()

	// 2. Layer 0 (Parallel Node B): Fetch User Orders
	fetchOrders := action.New("orders.fetch", func(ctx context.Context, nCtx *dag.NodeContext) (OrderSummary, error) {
		time.Sleep(30 * time.Millisecond)
		return OrderSummary{TotalSpent: 1249.99, Count: 8}, nil
	}).Build()

	// 3. Layer 1 (Dependent Node C): AI Synthesis
	// Runs ONLY after both User and Orders finish.
	generateInsight := action.New("ai.synthesize", func(ctx context.Context, nCtx *dag.NodeContext) (string, error) {
		user, err := dag.GetNodeOutput[UserProfile](nCtx.Input, "user_node")
		if err != nil {
			return "", err
		}

		orders, err := dag.GetNodeOutput[OrderSummary](nCtx.Input, "orders_node")
		if err != nil {
			return "", err
		}

		// Synthesize output
		return fmt.Sprintf("Customer %s [%s tier] has placed %d orders totaling $%.2f. Retention risk: LOW.",
			user.Name, user.Tier, orders.Count, orders.TotalSpent), nil
	}).Build()

	// 4. Compile the DAG
	graph, err := dag.New("customer_intelligence_flow").
		AddNode("user_node", "user_data", fetchUser).
		AddNode("orders_node", "order_data", fetchOrders).
		AddNode("ai_node", "ai_result", generateInsight).
		AddEdge("user_node", "ai_node").   // ai_node depends on user_node
		AddEdge("orders_node", "ai_node"). // ai_node depends on orders_node
		Compile()

	if err != nil {
		panic(err)
	}

	// 5. Execute Graph
	initialState := dag.AcquireState()
	defer initialState.Release()

	finalState, err := graph.Execute(ctx, initialState)
	if err != nil {
		panic(err)
	}
	defer finalState.Release()

	// 6. Read typed output
	result, err := dag.GetNodeOutput[string](finalState, "ai_node")
	if err != nil {
		panic(err)
	}

	fmt.Println("🚀 DAG Execution Result:")
	fmt.Println(result)
}
