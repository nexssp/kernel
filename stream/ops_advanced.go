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

// Debounce drops items followed by another item within the interval.
func Debounce[T any](ctx context.Context, interval time.Duration) StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			// Own cancellation scope: guarantees the feeder goroutine unblocks
			// even if yield stops the range early and the caller's ctx never cancels.
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

			var timer *time.Timer
			var lastItem T
			var hasPending bool

			for {
				select {
				case <-opCtx.Done():
					var zero T
					yield(zero, opCtx.Err())
					return
				case p, ok := <-ch:
					if !ok {
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
						flush()
						return
					}
					if p.err != nil {
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
