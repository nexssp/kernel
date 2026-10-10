package timing_test

import (
	"testing"
	"time"

	"github.com/nexssp/kernel/timing"
	"github.com/nexssp/kernel/xtest"
)

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
