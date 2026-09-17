package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestHistory(t *testing.T) {
	t.Parallel()

	act, hist := action.New("hist.test", func(_ context.Context, req int) (int, error) {
		return req * 10, nil
	}).WithHistory(5)

	built := act.Build()

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
}

func TestRecordHistoryAndWithHistory_ShareInstance(t *testing.T) {
	t.Parallel()

	// WithHistory — zwrócony handle
	_, histA := action.New("hist.a", func(context.Context, int) (int, error) {
		return 1, nil
	}).WithHistory(5)

	// RecordHistory — uchwyt z BuiltAction
	actB := action.New("hist.b", func(context.Context, int) (int, error) {
		return 2, nil
	}).RecordHistory(5).Build()

	histB := actB.History()

	if histA == nil || histB == nil {
		t.Fatal("expected both histories to be non-nil")
	}
}
