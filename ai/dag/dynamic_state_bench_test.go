package dag_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func BenchmarkDynamicExecute(b *testing.B) {
	for _, initialSize := range []int{0, 128, 1024} {
		for _, outputCount := range []int{16, 64, 256} {
			b.Run(fmt.Sprintf("initial=%d/outputs=%d", initialSize, outputCount), func(b *testing.B) {
				graph := buildDynamicBenchmarkGraph(b, outputCount)
				initial := dag.AcquireState()
				defer initial.Release()
				for index := range initialSize {
					initial.Set(fmt.Sprintf("seed-%d", index), index)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					state, err := graph.Execute(ctx, initial.AsRead())
					if err != nil {
						b.Fatal(err)
					}
					state.Release()
				}
			})
		}
	}
}

func buildDynamicBenchmarkGraph(b *testing.B, outputCount int) *dag.DAG {
	b.Helper()
	builder := dag.New(b.Name()).WithMaxSteps(outputCount)
	for index := range outputCount {
		id := fmt.Sprintf("node-%03d", index)
		nextID := "end"
		if index+1 < outputCount {
			nextID = fmt.Sprintf("node-%03d", index+1)
		}
		act := action.New(id, func(context.Context, dag.ReadState) (string, error) {
			return "value", nil
		}).Build()
		builder.AddNode(id, fmt.Sprintf("output-%03d", index), act)
		builder.AddConditionalEdge(id, func(context.Context, dag.ReadState) (string, error) {
			return nextID, nil
		})
	}
	graph, err := builder.Compile()
	if err != nil {
		b.Fatal(err)
	}
	return graph
}
