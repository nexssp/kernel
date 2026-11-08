package stream

import (
	"context"
	"errors"
	"iter"
	"time"
)

const maxTicksPerSecond = 1_000_000_000

// ErrInvalidTickRate is returned when a requested rate cannot produce a
// positive ticker interval at nanosecond resolution.
var ErrInvalidTickRate = errors.New("stream: tick rate must be between 1 and 1000000000 Hz")

// Ticks yields a fixed-timestep duration (e.g., 16.6ms for 60Hz).
// It decouples physical wall-clock time from logical engine ticks.
// Nonpositive rates retain the legacy empty-sequence behavior; an invalid
// positive rate yields ErrInvalidTickRate once. Use TicksChecked to validate
// the rate before starting iteration.
//
// Lifecycle: the underlying time.Ticker is stopped when the consumer
// breaks out of the range loop OR when ctx is canceled. Callers MUST
// ensure the context is canceled or the range loop terminates to avoid
// leaking the ticker.
func Ticks(ctx context.Context, hz int, maxCatchup time.Duration) iter.Seq2[time.Duration, error] {
	if hz <= 0 {
		return func(func(time.Duration, error) bool) {}
	}
	seq, err := TicksChecked(ctx, hz, maxCatchup)
	if err == nil {
		return seq
	}
	return func(yield func(time.Duration, error) bool) {
		yield(0, err)
	}
}

// TicksChecked validates the tick rate before constructing the iterator. The
// rate must yield a positive nanosecond interval; use Ticks when an iterator
// that reports invalid positive rates through its error result is preferred.
func TicksChecked(ctx context.Context, hz int, maxCatchup time.Duration) (iter.Seq2[time.Duration, error], error) {
	if hz <= 0 || hz > maxTicksPerSecond {
		return nil, ErrInvalidTickRate
	}
	interval := time.Second / time.Duration(hz)
	if interval <= 0 {
		return nil, ErrInvalidTickRate
	}
	return ticksAtInterval(ctx, interval, maxCatchup), nil
}

func ticksAtInterval(ctx context.Context, interval, maxCatchup time.Duration) iter.Seq2[time.Duration, error] {
	return func(yield func(time.Duration, error) bool) {
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
