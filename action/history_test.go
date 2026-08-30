package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestHistory(t *testing.T) {
	t.Parallel()
	act, hist := action.New("hist.test", func(ctx context.Context, req int) (int, error) {
		return req * 10, nil
	}).WithHistory(5)
	built := act.Build()

	// Execute 7 times (capacity is 5)
	for i := range 7 {
		_, err := built.Do(context.Background(), i)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	snap := hist.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("expected 5 records, got %d", len(snap))
	}
	// The oldest 2 should have been evicted; we should have records for req=2..6
	if snap[0].Req != 2 || snap[4].Req != 6 {
		t.Errorf("unexpected ring buffer contents: %+v", snap)
	}
}
