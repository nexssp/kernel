package main

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xtest/ktest"
)

func TestLazyETLPipeline(t *testing.T) {
	ctx := context.Background()
	pipeline := BuildPipeline()

	// Request only 15 items to easily verify the math
	seq, err := pipeline.Do(ctx, 15)
	ktest.RequireNoError(t, err)

	var results []LogEntry
	for item, err := range seq {
		ktest.RequireNoError(t, err)
		results = append(results, item)
	}

	// Out of 15 generated elements (IDs 1 through 15),
	// the following multiples match ERROR (5) or FATAL (7):
	// ID: 5 (ERROR)
	// ID: 7 (FATAL)
	// ID: 10 (ERROR)
	// ID: 14 (FATAL)
	// ID: 15 (ERROR)
	// Total elements expected: 5
	ktest.RequireEqual(t, len(results), 5)

	expectedIDs := []int{5, 7, 10, 14, 15}

	for i, res := range results {
		// Verify Filter logic
		ktest.RequireEqual(t, res.ID, expectedIDs[i])
		if res.Level != "ERROR" && res.Level != "FATAL" {
			t.Fatalf("expected ERROR or FATAL, got %s", res.Level)
		}

		// Verify Map (Redaction) logic
		ktest.RequireEqual(t, res.Message, "[REDACTED] Module error")
	}
}
