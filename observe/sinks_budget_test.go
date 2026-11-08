package observe_test

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/nexssp/kernel/observe"
	"github.com/nexssp/kernel/ringbuf"
)

func TestNewMemorySinkReportsMemoryBudgetRejection(t *testing.T) {
	eventSize := uint64(unsafe.Sizeof(observe.Event{}))
	capacity := int(ringbuf.DefaultMemoryBudgetBytes/eventSize) + 1

	sink, err := observe.NewMemorySinkChecked(capacity)
	if sink != nil || !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
		t.Fatalf("NewMemorySinkChecked() = (%v, %v), want (nil, ErrMemoryBudgetExceeded)", sink, err)
	}
	if sink := observe.NewMemorySink(capacity); sink != nil {
		t.Fatal("NewMemorySink() returned an inert sink after budget rejection")
	}
}

func TestNewMemorySinkWithOptionsCreatesUsableSink(t *testing.T) {
	sink, err := observe.NewMemorySinkWithOptions(ringbuf.BufferOptions{
		Capacity:          1,
		MemoryBudgetBytes: 4096,
	})
	if err != nil {
		t.Fatalf("NewMemorySinkWithOptions() error = %v", err)
	}

	sink.Emit(t.Context(), observe.Event{Action: "test"})
	if got := sink.Events(); len(got) != 1 || got[0].Action != "test" {
		t.Fatalf("sink events = %#v, want one test event", got)
	}
}
