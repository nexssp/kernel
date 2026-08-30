package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestBuilder_RecordHistoryZeroCapacity(t *testing.T) {
	t.Parallel()

	act := action.New("hist.zero", func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	}).RecordHistory(0).Build()

	res, err := act.Do(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 10 {
		t.Fatalf("expected 10, got %d", res)
	}

	// With the fix, capacity is coerced to 1, so Snapshot returns exactly 1 record.
	if hist := act.History(); hist != nil {
		if len(hist.Snapshot()) != 1 {
			t.Fatalf("expected 1 recorded entry, got %d", len(hist.Snapshot()))
		}
	}
}
