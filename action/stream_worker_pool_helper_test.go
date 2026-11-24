package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestStreamWorkerPoolNilActionYieldsOneError(t *testing.T) {
	op := action.StreamWorkerPool[int, int](context.Background(), 2, 0, nil)
	out := op(action.StreamFromSlice([]int{1, 2}))

	var values []int
	errorCount := 0
	for value, err := range out {
		if err != nil {
			errorCount++
			continue
		}
		values = append(values, value)
	}
	if len(values) != 0 || errorCount != 1 {
		t.Fatalf("values = %v, error count = %d; want no values and one error", values, errorCount)
	}
}
