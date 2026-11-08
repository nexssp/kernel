package stream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/stream"
)

func TestTicks_BasicInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	seq := stream.Ticks(ctx, 100, 50*time.Millisecond) // 100Hz = 10ms per tick

	count := 0
	for dt, err := range seq {
		if err != nil {
			break
		}
		if dt != 10*time.Millisecond {
			t.Fatalf("unexpected dt: %v", dt)
		}
		count++
		if count >= 3 {
			break // Proves early exit releases ticker cleanly
		}
	}

	if count < 3 {
		t.Fatalf("expected at least 3 ticks, got %d", count)
	}
}

func TestTicksCheckedRejectsRatesAboveOneGHz(t *testing.T) {
	for _, hz := range []int{1_000_000_001, int(^uint(0) >> 1)} {
		if _, err := stream.TicksChecked(context.Background(), hz, time.Second); !errors.Is(err, stream.ErrInvalidTickRate) {
			t.Fatalf("TicksChecked(%d) error = %v, want ErrInvalidTickRate", hz, err)
		}

		yieldedError := false
		for _, err := range stream.Ticks(context.Background(), hz, time.Second) {
			if !errors.Is(err, stream.ErrInvalidTickRate) {
				t.Fatalf("Ticks(%d) yielded error %v, want ErrInvalidTickRate", hz, err)
			}
			yieldedError = true
			break
		}
		if !yieldedError {
			t.Fatalf("Ticks(%d) did not report an invalid positive rate", hz)
		}
	}
}

func TestTicksCheckedSupportsOneGHz(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seq, err := stream.TicksChecked(ctx, 1_000_000_000, time.Nanosecond)
	if err != nil {
		t.Fatalf("TicksChecked(1GHz) error = %v", err)
	}
	for dt, err := range seq {
		if err != nil {
			t.Fatalf("1GHz tick error = %v", err)
		}
		if dt != time.Nanosecond {
			t.Fatalf("1GHz tick duration = %v, want 1ns", dt)
		}
		cancel()
		return
	}
	t.Fatal("1GHz iterator returned without producing a tick")
}

func TestTicksNonpositiveRateRemainsEmpty(t *testing.T) {
	for _, hz := range []int{0, -1} {
		for range stream.Ticks(context.Background(), hz, time.Second) {
			t.Fatalf("Ticks(%d) yielded a value, want the legacy empty sequence", hz)
		}
	}
}
