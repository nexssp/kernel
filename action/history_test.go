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

func TestHistoryMiddlewareCapturesRequestValueAndSharedSliceMutation(t *testing.T) {
	type request struct {
		Name string
		Tags []string
	}

	history := action.NewHistory[request, int](1)
	req := request{Name: "original", Tags: []string{"before"}}
	next := action.HistoryMiddleware[request, int](history)(func(_ context.Context, req request) (int, error) {
		req.Name = "handler-local"
		req.Tags[0] = "mutated"
		return 1, nil
	})
	if _, err := next(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	records := history.Snapshot()
	if len(records) != 1 {
		t.Fatalf("history has %d records, want 1", len(records))
	}
	if got := records[0].Req; got.Name != "original" || got.Tags[0] != "mutated" {
		t.Fatalf("recorded request = %+v, want original value field and mutated shared slice", got)
	}
}

func TestRecordHistoryAndWithHistory_ShareInstance(t *testing.T) {
	t.Parallel()

	_, histA := action.New("hist.a", func(context.Context, int) (int, error) {
		return 1, nil
	}).WithHistory(5)

	actB := action.New("hist.b", func(context.Context, int) (int, error) {
		return 2, nil
	}).RecordHistory(5).Build()

	histB := actB.History()

	if histA == nil || histB == nil {
		t.Fatal("expected both histories to be non-nil")
	}
}
