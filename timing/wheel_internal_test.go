package timing

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestUnlinkAndFreeClearsTimerIdentityAndPayload(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		wheel := New(time.Nanosecond, 8, 2)
		id, err := wheel.Schedule(time.Nanosecond, new(int))
		if err != nil {
			t.Fatal(err)
		}
		index := int(id & TimerID(math.MaxUint32))
		if canceled && !wheel.Cancel(id) {
			t.Fatal("Cancel() failed for active timer")
		}
		got := wheel.Advance(time.Nanosecond, nil)
		if canceled && len(got) != 0 {
			t.Fatalf("canceled timer fired: %v", got)
		}
		if !canceled && len(got) != 1 {
			t.Fatalf("expired timers = %d, want 1", len(got))
		}
		if node := wheel.nodes[index]; node.id != 0 || node.payload != nil || node.canceled {
			t.Fatalf("freed timer node retained state: %+v", node)
		}
		if wheel.Cancel(id) {
			t.Fatal("Cancel() succeeded for an expired or freed timer ID")
		}
	}
}

func TestScheduleStopsBeforeTimerIDGenerationWrap(t *testing.T) {
	wheel := New(time.Nanosecond, 8, 2)
	wheel.epoch = math.MaxUint32 - 1
	id, err := wheel.Schedule(time.Nanosecond, "last generation")
	if err != nil {
		t.Fatal(err)
	}
	if generation := uint32(id >> 32); generation != math.MaxUint32 {
		t.Fatalf("last generation = %d, want %d", generation, uint32(math.MaxUint32))
	}
	freeHead := wheel.freeHead
	if _, err := wheel.Schedule(time.Nanosecond, "must not alias"); !errors.Is(err, ErrTimerIDExhausted) {
		t.Fatalf("Schedule() at generation exhaustion error = %v, want ErrTimerIDExhausted", err)
	}
	if wheel.freeHead != freeHead {
		t.Fatal("failed Schedule() consumed a free node")
	}
}
