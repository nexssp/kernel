package timing_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nexssp/kernel/timing"
	"github.com/nexssp/kernel/xtest"
)

func TestNewCheckedRejectsInvalidDimensions(t *testing.T) {
	tests := []struct {
		name         string
		resolution   time.Duration
		slots        int
		nodeCapacity int
	}{
		{name: "zero resolution", resolution: 0, slots: 8, nodeCapacity: 8},
		{name: "negative resolution", resolution: -time.Second, slots: 8, nodeCapacity: 8},
		{name: "zero slots", resolution: time.Second, slots: 0, nodeCapacity: 8},
		{name: "negative slots", resolution: time.Second, slots: -1, nodeCapacity: 8},
		{name: "zero node capacity", resolution: time.Second, slots: 8, nodeCapacity: 0},
		{name: "negative node capacity", resolution: time.Second, slots: 8, nodeCapacity: -1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wheel, err := timing.NewChecked(test.resolution, test.slots, test.nodeCapacity)
			if err == nil || wheel != nil {
				t.Fatalf("NewChecked() = (%v, %v), want (nil, error)", wheel, err)
			}
			if wheel := timing.New(test.resolution, test.slots, test.nodeCapacity); wheel != nil {
				t.Fatal("New() returned a wheel for invalid dimensions")
			}
		})
	}
}

func TestNewCheckedAcceptsValidDimensions(t *testing.T) {
	wheel, err := timing.NewChecked(10*time.Millisecond, 8, 16)
	if err != nil || wheel == nil {
		t.Fatalf("NewChecked() = (%v, %v), want a wheel and no error", wheel, err)
	}
}

func TestWheel_LifecycleAndCancel(t *testing.T) {
	w := timing.New(10*time.Millisecond, 256, 1024)

	id1, err := w.Schedule(50*time.Millisecond, "buff1")
	if err != nil {
		t.Fatalf("unexpected error scheduling buff1: %v", err)
	}

	_, err = w.Schedule(50*time.Millisecond, "buff2")
	if err != nil {
		t.Fatalf("unexpected error scheduling buff2: %v", err)
	}

	if !w.Cancel(id1) {
		t.Fatal("expected successful cancellation")
	}
	if w.Cancel(id1) {
		t.Fatal("second cancel should fail")
	}

	var dst []any
	dst = w.Advance(60*time.Millisecond, dst)

	if len(dst) != 1 || dst[0] != "buff2" {
		t.Fatalf("expected only buff2 to expire, got: %v", dst)
	}
}

// TestWheel_ExactCycleDelay verifies that delays matching exact multiples
// of slots * resolution fire on schedule rather than one cycle late.
func TestWheel_ExactCycleDelay(t *testing.T) {
	const (
		resolution = 10 * time.Millisecond
		slots      = 8
	)
	w := timing.New(resolution, slots, 64)

	_, err := w.Schedule(80*time.Millisecond, "exact_cycle")
	if err != nil {
		t.Fatal(err)
	}

	var dst []any
	dst = w.Advance(80*time.Millisecond, dst)

	if len(dst) != 1 || dst[0] != "exact_cycle" {
		t.Fatalf("expected exact cycle timer to fire at 80ms, got: %v", dst)
	}
}

func TestWheel_FractionalTickDelayDoesNotExpireEarly(t *testing.T) {
	const resolution = 10 * time.Millisecond

	tests := []struct {
		name         string
		elapsed      time.Duration
		delay        time.Duration
		beforeExpiry time.Duration
	}{
		{name: "fractional delay", delay: 15 * time.Millisecond, beforeExpiry: 15 * time.Millisecond},
		{name: "fractional delay after partial tick", elapsed: 9 * time.Millisecond, delay: 5 * time.Millisecond, beforeExpiry: time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := timing.New(resolution, 8, 8)
			var dst []any
			if test.elapsed > 0 {
				dst = w.Advance(test.elapsed, dst)
			}
			if _, err := w.Schedule(test.delay, "deadline"); err != nil {
				t.Fatal(err)
			}

			dst = w.Advance(test.beforeExpiry, dst)
			if len(dst) != 0 {
				t.Fatalf("timer expired before its rounded-up deadline: %v", dst)
			}
			dst = w.Advance(resolution, dst)
			if len(dst) != 1 || dst[0] != "deadline" {
				t.Fatalf("timer did not expire on the next eligible tick: %v", dst)
			}
		})
	}
}

func TestWheel_DoubleCycleDelay(t *testing.T) {
	const (
		resolution = 10 * time.Millisecond
		slots      = 8
	)
	w := timing.New(resolution, slots, 64)

	_, err := w.Schedule(160*time.Millisecond, "double_cycle")
	if err != nil {
		t.Fatal(err)
	}

	var dst []any
	dst = w.Advance(80*time.Millisecond, dst)
	if len(dst) != 0 {
		t.Fatalf("timer fired prematurely at cycle 1: %v", dst)
	}

	dst = w.Advance(80*time.Millisecond, dst)
	if len(dst) != 1 || dst[0] != "double_cycle" {
		t.Fatalf("expected timer to fire at 160ms, got: %v", dst)
	}
}

func TestWheel_ExtremeDelayDoesNotOverflow(t *testing.T) {
	w := timing.New(time.Nanosecond, 8, 1)
	w.Advance(time.Nanosecond, nil)
	if _, err := w.Schedule(time.Duration(1<<63-1), "far-future"); err != nil {
		t.Fatalf("Schedule(max duration) error = %v", err)
	}
	if got := w.Advance(time.Nanosecond, nil); len(got) != 0 {
		t.Fatalf("extreme timer expired early: %v", got)
	}
}

func TestWheelLargeAdvancePreservesTimerOrderAndFutureWork(t *testing.T) {
	const huge = time.Duration(1 << 40)
	wheel := timing.New(time.Nanosecond, 4, 8)
	for _, timer := range []struct {
		delay   time.Duration
		payload string
	}{
		{delay: time.Nanosecond, payload: "first"},
		{delay: 5 * time.Nanosecond, payload: "older"},
		{delay: 5 * time.Nanosecond, payload: "newer"},
		{delay: 2 * time.Nanosecond, payload: "second-slot"},
	} {
		if _, err := wheel.Schedule(timer.delay, timer.payload); err != nil {
			t.Fatal(err)
		}
	}
	canceled, err := wheel.Schedule(9*time.Nanosecond, "canceled")
	if err != nil {
		t.Fatal(err)
	}
	if !wheel.Cancel(canceled) {
		t.Fatal("Cancel() failed for a scheduled timer")
	}
	if _, err := wheel.Schedule(huge+4, "future"); err != nil {
		t.Fatal(err)
	}

	got := wheel.Advance(huge, nil)
	want := []any{"first", "second-slot", "newer", "older"}
	if !slices.Equal(got, want) {
		t.Fatalf("large Advance() = %v, want %v", got, want)
	}
	if got = wheel.Advance(3*time.Nanosecond, got); !slices.Equal(got, want) {
		t.Fatalf("future timer fired early: %v", got)
	}
	got = wheel.Advance(time.Nanosecond, got)
	want = append(want, "future")
	if !slices.Equal(got, want) {
		t.Fatalf("future timer was lost or misordered: %v, want %v", got, want)
	}
}

func TestWheelLargeAdvanceMatchesTickByTickReference(t *testing.T) {
	newWheel := func(t *testing.T) *timing.Wheel {
		t.Helper()
		wheel := timing.New(3*time.Nanosecond, 5, 32)
		var ids []timing.TimerID
		for i, delay := range []time.Duration{
			time.Nanosecond, 2 * time.Nanosecond, 3 * time.Nanosecond,
			4 * time.Nanosecond, 5 * time.Nanosecond, 7 * time.Nanosecond,
			8 * time.Nanosecond, 15 * time.Nanosecond, 16 * time.Nanosecond,
			17 * time.Nanosecond, 100 * time.Nanosecond, 300 * time.Nanosecond,
		} {
			id, err := wheel.Schedule(delay, i)
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		for _, index := range []int{2, 8} {
			if !wheel.Cancel(ids[index]) {
				t.Fatalf("Cancel(%d) failed", index)
			}
		}
		return wheel
	}

	fast := newWheel(t)
	slow := newWheel(t)
	fastResult := fast.Advance(3000*time.Nanosecond, nil)
	var slowResult []any
	for range 1000 {
		slowResult = slow.Advance(3*time.Nanosecond, slowResult)
	}
	if !slices.Equal(fastResult, slowResult) {
		t.Fatalf("fast-forward = %v, tick-by-tick = %v", fastResult, slowResult)
	}
}

func TestWheelAdvanceCheckedRejectsNegativeDeltaWithoutMutation(t *testing.T) {
	wheel := timing.New(time.Nanosecond, 8, 2)
	if _, err := wheel.Schedule(2*time.Nanosecond, "pending"); err != nil {
		t.Fatal(err)
	}
	prior := []any{"existing"}
	got, err := wheel.AdvanceChecked(-time.Nanosecond, prior)
	if !errors.Is(err, timing.ErrNegativeAdvance) {
		t.Fatalf("AdvanceChecked() error = %v, want ErrNegativeAdvance", err)
	}
	if !slices.Equal(got, prior) {
		t.Fatalf("AdvanceChecked() result = %v, want unchanged %v", got, prior)
	}
	got = wheel.Advance(2*time.Nanosecond, nil)
	if !slices.Equal(got, []any{"pending"}) {
		t.Fatalf("timer state changed after rejected delta: %v", got)
	}
}

func TestWheelAdvanceMaxDurationWithRemainder(t *testing.T) {
	wheel := timing.New(2*time.Nanosecond, 8, 1)
	wheel.Advance(time.Nanosecond, nil)
	if _, err := wheel.Schedule(time.Duration(1<<63-1), "max-duration"); err != nil {
		t.Fatal(err)
	}
	got := wheel.Advance(time.Duration(1<<63-1), nil)
	if !slices.Equal(got, []any{"max-duration"}) {
		t.Fatalf("maximum-duration Advance() = %v, want due timer", got)
	}
}

func FuzzWheelFastForwardMatchesTickByTick(f *testing.F) {
	for _, seed := range []uint8{65, 80, 128, 255} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, rawTicks uint8) {
		ticks := int(rawTicks)
		if ticks <= 64 {
			return
		}
		newWheel := func() *timing.Wheel {
			wheel := timing.New(time.Nanosecond, 5, 32)
			var ids []timing.TimerID
			for i, delay := range []time.Duration{
				time.Nanosecond, 2 * time.Nanosecond, 3 * time.Nanosecond,
				4 * time.Nanosecond, 5 * time.Nanosecond, 7 * time.Nanosecond,
				8 * time.Nanosecond, 15 * time.Nanosecond, 16 * time.Nanosecond,
				17 * time.Nanosecond, 100 * time.Nanosecond, 300 * time.Nanosecond,
			} {
				id, err := wheel.Schedule(delay, i)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			for _, index := range []int{2, 8} {
				if !wheel.Cancel(ids[index]) {
					t.Fatalf("Cancel(%d) failed", index)
				}
			}
			return wheel
		}

		fast, slow := newWheel(), newWheel()
		fastResult := fast.Advance(time.Duration(ticks), nil)
		var slowResult []any
		for range ticks {
			slowResult = slow.Advance(time.Nanosecond, slowResult)
		}
		if !slices.Equal(fastResult, slowResult) {
			t.Fatalf("fast-forward %d = %v, tick-by-tick = %v", ticks, fastResult, slowResult)
		}
	})
}

func TestWheel_ZeroAlloc(t *testing.T) {
	w := timing.New(10*time.Millisecond, 256, 1024)
	dst := make([]any, 0, 16)

	xtest.RequireZeroAlloc(t, 1000, func() {
		id, _ := w.Schedule(50*time.Millisecond, "buff")
		w.Cancel(id)

		_, _ = w.Schedule(20*time.Millisecond, "debuff")

		dst = w.Advance(30*time.Millisecond, dst[:0])
		if len(dst) != 1 {
			t.Fatal("missing expiration")
		}
	})
}

func BenchmarkWheelLargeCatchup(b *testing.B) {
	const largeDelta = time.Duration(1 << 40)
	wheel := timing.New(time.Nanosecond, 64, 1)
	dst := make([]any, 0, 1)
	advance := func() {
		if _, err := wheel.Schedule(time.Nanosecond, "timer"); err != nil {
			b.Fatal(err)
		}
		dst = wheel.Advance(largeDelta, dst[:0])
		if len(dst) != 1 {
			b.Fatal("large catch-up did not expire its timer")
		}
	}
	advance()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		advance()
	}
}
