package stream

import (
	"context"
	"iter"
	"time"
)

// Ticks yields a fixed-timestep duration (e.g., 16.6ms for 60Hz).
// It decouples physical wall-clock time from logical engine ticks.
//
// Lifecycle: the underlying time.Ticker is stopped when the consumer
// breaks out of the range loop OR when ctx is canceled. Callers MUST
// ensure the context is canceled or the range loop terminates to avoid
// leaking the ticker.
func Ticks(ctx context.Context, hz int, maxCatchup time.Duration) iter.Seq2[time.Duration, error] {
	return func(yield func(time.Duration, error) bool) {
		if hz <= 0 {
			return
		}

		interval := time.Second / time.Duration(hz)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var accumulated time.Duration
		lastTime := time.Now()

		for {
			select {
			case <-ctx.Done():
				yield(0, ctx.Err())
				return
			case now := <-ticker.C:
				dt := now.Sub(lastTime)
				lastTime = now
				accumulated += dt

				if accumulated > maxCatchup {
					accumulated = maxCatchup // Prevent lag death spiral
				}

				for accumulated >= interval {
					accumulated -= interval
					if !yield(interval, nil) {
						return
					}
				}
			}
		}
	}
}
