package action_test

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ringbuf"
)

type oversizedHistoryRequest [1 << 20]byte

func oversizedHistoryCapacity() int {
	recordSize := uint64(unsafe.Sizeof(action.Record[oversizedHistoryRequest, int]{}))
	return int(ringbuf.DefaultMemoryBudgetBytes/recordSize) + 1
}

func TestNewHistoryReportsMemoryBudgetRejection(t *testing.T) {
	capacity := oversizedHistoryCapacity()

	history, err := action.NewHistoryChecked[oversizedHistoryRequest, int](capacity)
	if history != nil || !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
		t.Fatalf("NewHistoryChecked() = (%v, %v), want (nil, ErrMemoryBudgetExceeded)", history, err)
	}
	if history := action.NewHistory[oversizedHistoryRequest, int](capacity); history != nil {
		t.Fatal("NewHistory() returned an inert history after budget rejection")
	}
}

func TestNewHistoryWithOptionsCreatesUsableHistory(t *testing.T) {
	history, err := action.NewHistoryWithOptions[int, int](ringbuf.BufferOptions{
		Capacity:          1,
		MemoryBudgetBytes: 4096,
	})
	if err != nil {
		t.Fatalf("NewHistoryWithOptions() error = %v", err)
	}

	history.Push(action.Record[int, int]{Req: 3, Res: 7})
	if got := history.Snapshot(); len(got) != 1 || got[0].Req != 3 || got[0].Res != 7 {
		t.Fatalf("history snapshot = %#v, want one record for 3 -> 7", got)
	}
}

func TestRecordHistoryBudgetRejectionPreventsActionExecution(t *testing.T) {
	calls := 0
	act := action.New("history-budget", func(context.Context, oversizedHistoryRequest) (int, error) {
		calls++
		return 7, nil
	}).RecordHistory(oversizedHistoryCapacity()).Build()

	if act.History() != nil {
		t.Fatal("History() returned a handle after the requested history was rejected")
	}
	if _, err := act.Do(context.Background(), oversizedHistoryRequest{}); !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
		t.Fatalf("Do() error = %v, want ErrMemoryBudgetExceeded", err)
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0 after history configuration failure", calls)
	}
}

func TestRecordHistoryCheckedReturnsBudgetError(t *testing.T) {
	builder, err := action.New("history-budget-checked", func(context.Context, oversizedHistoryRequest) (int, error) {
		return 7, nil
	}).RecordHistoryChecked(oversizedHistoryCapacity())
	if builder == nil || !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
		t.Fatalf("RecordHistoryChecked() = (%v, %v), want a builder and ErrMemoryBudgetExceeded", builder, err)
	}
}

func TestRecordHistoryWithOptionsCreatesUsableHistory(t *testing.T) {
	builder, err := action.New("history-budget-options", func(_ context.Context, req int) (int, error) {
		return req * 2, nil
	}).RecordHistoryWithOptions(ringbuf.BufferOptions{
		Capacity:          1,
		MemoryBudgetBytes: 4096,
	})
	if err != nil {
		t.Fatalf("RecordHistoryWithOptions() error = %v", err)
	}

	act := builder.Build()
	if _, err := act.Do(context.Background(), 3); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got := act.History().Snapshot(); len(got) != 1 || got[0].Req != 3 || got[0].Res != 6 {
		t.Fatalf("history snapshot = %#v, want one record for 3 -> 6", got)
	}
}
