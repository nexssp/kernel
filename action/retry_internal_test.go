package action

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitForRetryCancellationWhenTimerReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	timerReady := make(chan time.Time, 1)
	timerReady <- time.Time{}

	if err := waitForRetry(ctx, timerReady); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForRetry() error = %v, want context.Canceled", err)
	}
}
