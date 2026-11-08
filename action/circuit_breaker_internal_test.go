package action

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/xerr"
)

func TestAdaptiveTimeoutAfterFailuresSaturates(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	maxInt := int(^uint(0) >> 1)
	wantAtMaxInt := maxDuration
	if strconv.IntSize == 32 {
		wantAtMaxInt = 10*time.Nanosecond*time.Duration(1+int64(maxInt)/10) + time.Duration(int64(maxInt)%10)
	}
	tests := []struct {
		name     string
		initial  time.Duration
		failures int
		want     time.Duration
	}{
		{name: "no failures", initial: time.Second, want: time.Second},
		{name: "one failure", initial: time.Second, failures: 1, want: 1100 * time.Millisecond},
		{name: "nine failures", initial: time.Second, failures: 9, want: 1900 * time.Millisecond},
		{name: "ten failures", initial: time.Second, failures: 10, want: 2 * time.Second},
		{name: "maximum int failure count", initial: 10 * time.Nanosecond, failures: maxInt, want: wantAtMaxInt},
		{name: "maximum initial timeout", initial: maxDuration, failures: 1, want: maxDuration},
		{name: "nonpositive initial timeout", initial: -time.Second, failures: 1, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := adaptiveTimeoutAfterFailures(test.initial, test.failures); got != test.want {
				t.Fatalf("adaptiveTimeoutAfterFailures(%d, %d) = %d, want %d", test.initial, test.failures, got, test.want)
			}
			if got := adaptiveTimeoutAfterFailures(test.initial, test.failures); got < 0 {
				t.Fatalf("adaptive timeout = %d, must not be negative", got)
			}
		})
	}
}

func FuzzAdaptiveTimeoutNeverNegative(f *testing.F) {
	f.Add(int64(time.Nanosecond), 1)
	f.Add(int64(1<<63-1), int(^uint(0)>>1))
	f.Add(int64(-1), -1)

	f.Fuzz(func(t *testing.T, initial int64, failures int) {
		got := adaptiveTimeoutAfterFailures(time.Duration(initial), failures)
		if got < 0 {
			t.Fatalf("adaptiveTimeoutAfterFailures(%d, %d) = %d, want nonnegative", initial, failures, got)
		}
		if initial > 0 && got < time.Duration(initial) {
			t.Fatalf("adaptiveTimeoutAfterFailures(%d, %d) = %d, want at least initial", initial, failures, got)
		}
	})
}

func TestCircuitBreaker_GoexitDuringProbeDoesNotWedge(t *testing.T) {
	state, err := NewCircuitBreakerState("goexit-probe", CircuitBreakerConfig{
		FailureThreshold: 1,
		ResetTimeout:     time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	act := New("goexit-probe", func(context.Context, struct{}) (string, error) {
		switch calls.Add(1) {
		case 1:
			return "", xerr.Unavailable("open breaker")
		case 2:
			runtime.Goexit()
			return "", nil
		default:
			return "ok", nil
		}
	}).Use(CircuitBreaker[struct{}, string](state)).Build()

	if _, err := act.Do(context.Background(), struct{}{}); xerr.KindFrom(err) != xerr.KindUnavailable {
		t.Fatalf("initial failure = %v, want Unavailable", err)
	}
	state.mu.Lock()
	state.openedAt = time.Time{}
	state.mu.Unlock()

	probeExited := make(chan struct{})
	go func() {
		defer close(probeExited)
		_, _ = act.Do(context.Background(), struct{}{})
	}()
	select {
	case <-probeExited:
	case <-time.After(time.Second):
		t.Fatal("Goexit probe did not exit")
	}

	state.mu.Lock()
	gotState := state.state
	state.openedAt = time.Time{}
	state.mu.Unlock()
	if gotState != stateOpen {
		t.Fatalf("breaker state after Goexit probe = %v, want open", gotState)
	}
	if got, err := act.Do(context.Background(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("next half-open probe = (%q, %v), want (ok, nil)", got, err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
}

func TestAdaptive_GoexitDuringProbeDoesNotWedge(t *testing.T) {
	var calls atomic.Int32
	act := New("adaptive-goexit-probe", func(context.Context, struct{}) (string, error) {
		switch calls.Add(1) {
		case 1:
			return "", xerr.Unavailable("open breaker")
		case 2:
			runtime.Goexit()
			return "", nil
		default:
			return "ok", nil
		}
	}).Use(Adaptive[struct{}, string]("adaptive-goexit-probe", AdaptiveConfig{
		FailureThreshold: 1,
		ResetTimeout:     time.Nanosecond,
		InitialTimeout:   time.Hour,
	})).Build()

	if _, err := act.Do(context.Background(), struct{}{}); xerr.KindFrom(err) != xerr.KindUnavailable {
		t.Fatalf("initial failure = %v, want Unavailable", err)
	}
	resetTimer := time.NewTimer(time.Nanosecond)
	<-resetTimer.C

	probeExited := make(chan struct{})
	go func() {
		defer close(probeExited)
		_, _ = act.Do(context.Background(), struct{}{})
	}()
	select {
	case <-probeExited:
	case <-time.After(time.Second):
		t.Fatal("Adaptive Goexit probe did not exit")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler calls after Goexit probe = %d, want 2", got)
	}

	resetTimer = time.NewTimer(time.Nanosecond)
	<-resetTimer.C
	if got, err := act.Do(context.Background(), struct{}{}); err != nil || got != "ok" {
		t.Fatalf("next Adaptive half-open probe = (%q, %v), want (ok, nil)", got, err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
}
