package stream

import (
	"context"
	"errors"
	"iter"
	"time"
)

// Window groups items into overlapping sliding windows.
func Window[T any](size, step int) StreamOp[T, []T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[[]T, error] {
		return func(yield func([]T, error) bool) {
			if size <= 0 || step <= 0 {
				var zero []T
				yield(zero, errors.New("stream: window size and step must be positive"))
				return
			}
			buffer := make([]T, 0, size)
			for item, err := range up {
				if err != nil {
					var zero []T
					yield(zero, err)
					return
				}
				buffer = append(buffer, item)
				if len(buffer) == size {
					batch := make([]T, size)
					copy(batch, buffer)
					if !yield(batch, nil) {
						return
					}
					if step >= size {
						buffer = buffer[:0]
					} else {
						copy(buffer, buffer[step:])
						buffer = buffer[:size-step]
					}
				}
			}
			if len(buffer) > 0 {
				yield(buffer, nil)
			}
		}
	}
}

// Throttle drops items that arrive faster than the specified interval. Zero allocations.
func Throttle[T any](interval time.Duration) StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			var lastEmitted time.Time
			for item, err := range up {
				if err != nil {
					var zero T
					yield(zero, err)
					return
				}
				now := time.Now()
				if now.Sub(lastEmitted) >= interval {
					lastEmitted = now
					if !yield(item, nil) {
						return
					}
				}
			}
		}
	}
}

// debouncePayload is one item-or-error handed from the feeder
// goroutine to the timing loop. It is package-level so the two
// helpers below can share it without re-declaring the type inside
// each closure.
type debouncePayload[T any] struct {
	item T
	err  error
}

// Debounce drops items followed by another item within the interval.
//
// The operator is a feeder goroutine plus a timing loop:
//
//   - the feeder reads the upstream iterator and pushes items onto a
//     channel, closing the channel when the iterator is exhausted or
//     the context is canceled;
//   - the loop restarts a timer on every received item and yields the
//     item only when the timer fires without a newer item arriving.
//
// Splitting the two makes each side's branch count legible: the feeder
// has one select and one error check, the loop has one select and the
// pending/timer pair. Neither alone exceeds the complexity budget.
func Debounce[T any](ctx context.Context, interval time.Duration) StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			opCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			ch := make(chan debouncePayload[T])
			go feedDebounce(opCtx, up, ch)

			debounceLoop(opCtx, ch, interval, yield)
		}
	}
}

// feedDebounce forwards items from the upstream iterator onto ch,
// closing the channel on exit. A context cancellation or an iterator
// error terminates the feeder; the consumer distinguishes the two
// cases by inspecting opCtx.Err() when it observes the closed channel.
func feedDebounce[T any](
	ctx context.Context,
	up iter.Seq2[T, error],
	ch chan<- debouncePayload[T],
) {
	defer close(ch)
	for item, err := range up {
		select {
		case <-ctx.Done():
			return
		case ch <- debouncePayload[T]{item: item, err: err}:
		}
		if err != nil {
			return
		}
	}
}

// debounceLoop is the consumer side of Debounce. It holds the timer
// and the last-seen item, and yields the pending item only when the
// timer fires without a newer arrival.
//
// The `if ctx.Err() != nil` guard inside the closed-channel branch
// disambiguates "feeder exhausted normally" from "feeder was canceled
// because the caller's context ended". Without it, a canceled context
// would race the channel close and could flush a pending item the
// caller is no longer interested in.
func debounceLoop[T any](
	ctx context.Context,
	ch <-chan debouncePayload[T],
	interval time.Duration,
	yield func(T, error) bool,
) {
	var timer *time.Timer
	var lastItem T
	var hasPending bool
	// Stop the pending timer on every exit path.
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			var zero T
			yield(zero, ctx.Err())
			return
		case p, ok := <-ch:
			if !ok {
				if err := ctx.Err(); err != nil {
					var zero T
					yield(zero, err)
					return
				}
				if hasPending {
					yield(lastItem, nil)
				}
				return
			}
			if p.err != nil {
				var zero T
				yield(zero, p.err)
				return
			}
			lastItem = p.item
			hasPending = true
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(interval)
		case <-timerC(timer):
			if hasPending {
				if !yield(lastItem, nil) {
					return
				}
				hasPending = false
			}
		}
	}
}

func timerC(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

// BatchByTime groups items up to a maximum size or until the timeout interval elapses.
//
//nolint:gocyclo // batch/timeout state machine reads more clearly inline
func BatchByTime[T any](ctx context.Context, size int, timeout time.Duration) StreamOp[T, []T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[[]T, error] {
		return func(yield func([]T, error) bool) {
			if size <= 0 {
				var zero []T
				yield(zero, errors.New("stream: batch size must be positive"))
				return
			}
			opCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			type payload struct {
				item T
				err  error
			}
			ch := make(chan payload)

			go func() {
				defer close(ch)
				for item, err := range up {
					select {
					case <-opCtx.Done():
						return
					case ch <- payload{item, err}:
					}
					if err != nil {
						return
					}
				}
			}()

			batch := make([]T, 0, size)
			var timer *time.Timer

			flush := func() bool {
				if len(batch) > 0 {
					res := make([]T, len(batch))
					copy(res, batch)
					batch = batch[:0]
					return yield(res, nil)
				}
				return true
			}

			for {
				if timer == nil {
					timer = time.NewTimer(timeout)
				} else {
					timer.Reset(timeout)
				}

				select {
				case <-opCtx.Done():
					// Preserve accumulated items before the error.
					if !flush() {
						return
					}
					var zero []T
					yield(zero, opCtx.Err())
					return
				case <-timer.C:
					if !flush() {
						return
					}
				case p, ok := <-ch:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					if !ok {
						if err := opCtx.Err(); err != nil {
							var zero []T
							yield(zero, err)
							return
						}
						flush()
						return
					}
					if p.err != nil {
						// Preserve accumulated items before the error.
						if !flush() {
							return
						}
						var zero []T
						yield(zero, p.err)
						return
					}
					batch = append(batch, p.item)
					if len(batch) == size {
						if !flush() {
							return
						}
					}
				}
			}
		}
	}
}
