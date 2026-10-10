package stream_test

import (
	"context"
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
