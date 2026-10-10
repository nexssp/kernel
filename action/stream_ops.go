package action

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/nexssp/kernel/stream"
	"github.com/nexssp/kernel/xerr"
)

// ── Source helpers ───────────────────────────────────────────────────────────

// StreamFromSlice returns an iter.Seq2 that yields items from the slice.
//
// This is the canonical way to turn an in-memory collection into a stream
// source. The iterator is single-use (standard iter.Seq2 semantics): consuming
// it twice, or concurrently, is undefined behavior.
func StreamFromSlice[T any](items []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range items {
			if !yield(item, nil) {
				return
			}
		}
	}
}

// StreamFromFunc adapts a pull-based source into an iter.Seq2.
//
// The next callback is expected to return:
//
//	(item, nil)             → emit item and continue
//	(zero, io.EOF)          → stop cleanly, no error emitted downstream
//	(zero, other error)     → stop with error emitted downstream
//
// Typical usage — wrapping bufio.Scanner or a database cursor:
//
//	scanner := bufio.NewScanner(f)
//	seq := action.StreamFromFunc(func() (string, error) {
//	    if !scanner.Scan() {
//	        if err := scanner.Err(); err != nil {
//	            return "", err
//	        }
//	        return "", io.EOF
//	    }
//	    return scanner.Text(), nil
//	})
//
// io.EOF is treated as the normal termination signal and is never
// propagated downstream. Use a different error type for real failures.
func StreamFromFunc[T any](next func() (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for {
			item, err := next()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			if !yield(item, nil) {
				return
			}
		}
	}
}

// ── Operators ────────────────────────────────────────────────────────────────

// StreamFilter filters items through a pure predicate.
//
// Contract:
//   - Upstream errors propagate unchanged; the predicate does not see them.
//   - Returning false from the downstream yield stops the upstream.
//   - Zero allocations per item (predicate is typed, no boxing).
//
// The predicate must be pure: no I/O, no mutation of shared state.
// For side-effecting transforms that may fail, use a StreamMapAction
// (planned for F1) instead.
func StreamFilter[T any](pred func(T) bool) stream.StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			for item, err := range up {
				if err != nil {
					if !yield(item, err) {
						return
					}
					continue
				}
				if pred(item) {
					if !yield(item, nil) {
						return
					}
				}
			}
		}
	}
}

// StreamCollect buffers the entire upstream into a slice and yields it as
// a single item.
//
// UNBOUNDED. The caller is responsible for ensuring the input is bounded.
// Use StreamCollectN when the input size is not guaranteed.
//
// The buffer is grown dynamically. On upstream error, the error is yielded
// and no partial slice is produced.
func StreamCollect[T any]() stream.StreamOp[T, []T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[[]T, error] {
		return func(yield func([]T, error) bool) {
			var buf []T
			for item, err := range up {
				if err != nil {
					yield(nil, err)
					return
				}
				buf = append(buf, item)
			}
			yield(buf, nil)
		}
	}
}

// StreamCollectN is the bounded variant of StreamCollect.
//
// maxItems must be > 0. If maxItems <= 0, the returned operator emits a
// xerr.BadRequest error without consuming the upstream stream.
//
// If the upstream yields more than maxItems items, a xerr.Forbidden error
// is emitted and the stream terminates. Partial results are never returned.
// If the upstream yields exactly maxItems items, they are returned without
// error.
func StreamCollectN[T any](maxItems int) stream.StreamOp[T, []T] {
	if maxItems <= 0 {
		invalidLimitErr := xerr.BadRequest("action.StreamCollectN: maxItems must be > 0")
		return func(_ iter.Seq2[T, error]) iter.Seq2[[]T, error] {
			return func(yield func([]T, error) bool) {
				yield(nil, invalidLimitErr)
			}
		}
	}
	return func(up iter.Seq2[T, error]) iter.Seq2[[]T, error] {
		return func(yield func([]T, error) bool) {
			// Pre-allocate up to 1024 to avoid a huge allocation on a
			// pathological maxItems value, while avoiding repeated growth
			// on typical inputs.
			initial := min(maxItems, 1024)
			buf := make([]T, 0, initial)

			count := 0
			for item, err := range up {
				if err != nil {
					yield(nil, err)
					return
				}
				if count >= maxItems {
					yield(nil, xerr.Forbidden(
						fmt.Sprintf("collect: max items %d exceeded", maxItems),
					))
					return
				}
				buf = append(buf, item)
				count++
			}
			yield(buf, nil)
		}
	}
}

// StreamWorkerPool executes a typed BuiltAction concurrently across stream items with bounded concurrency.
func StreamWorkerPool[In, Out any](
	ctx context.Context,
	concurrency int,
	bufferCapacity int,
	act *BuiltAction[In, Out],
) stream.StreamOp[In, Out] {
	if act == nil {
		return func(_ iter.Seq2[In, error]) iter.Seq2[Out, error] {
			return func(yield func(Out, error) bool) {
				var zero Out
				yield(zero, errors.New("action.StreamWorkerPool: action is nil"))
			}
		}
	}
	return stream.WorkerPool(ctx, concurrency, bufferCapacity, act.Do)
}
