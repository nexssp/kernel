// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

// Package stream contains domain-neutral, typed lazy stream operators.
package stream

import (
	"context"
	"errors"
	"iter"
)

// StreamOp transforms a lazy stream without erasing its item types.
//
//nolint:revive // StreamOp is the public domain term used across kernel and Flow.
type StreamOp[In, Out any] func(iter.Seq2[In, error]) iter.Seq2[Out, error]

// Map transforms every successful item. It does not intercept errors.
func Map[In, Out any](fn func(In) Out) StreamOp[In, Out] {
	return func(up iter.Seq2[In, error]) iter.Seq2[Out, error] {
		return func(yield func(Out, error) bool) {
			for item, err := range up {
				if err != nil {
					var zero Out
					if !yield(zero, err) {
						return
					}
					continue
				}
				if !yield(fn(item), nil) {
					return
				}
			}
		}
	}
}

// MapE transforms every successful item and can stop with an error.
func MapE[In, Out any](fn func(In) (Out, error)) StreamOp[In, Out] {
	return func(up iter.Seq2[In, error]) iter.Seq2[Out, error] {
		return func(yield func(Out, error) bool) {
			for item, err := range up {
				if err != nil {
					var zero Out
					yield(zero, err)
					return
				}
				out, err := fn(item)
				if err != nil {
					var zero Out
					yield(zero, err)
					return
				}
				if !yield(out, nil) {
					return
				}
			}
		}
	}
}

// Filter forwards only items accepted by pred.
func Filter[T any](pred func(T) bool) StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			for item, err := range up {
				if err != nil {
					if !yield(item, err) {
						return
					}
					continue
				}
				if pred(item) && !yield(item, nil) {
					return
				}
			}
		}
	}
}

// Take forwards at most n successful items. n <= 0 yields an empty stream.
func Take[T any](n int) StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			if n <= 0 {
				return
			}
			count := 0
			for item, err := range up {
				if !yield(item, err) {
					return
				}
				if err == nil {
					count++
					if count == n {
						return
					}
				}
			}
		}
	}
}

// Batch groups successful items into bounded batches. A final partial batch is
// emitted when the upstream ends normally.
func Batch[T any](size int) StreamOp[T, []T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[[]T, error] {
		return func(yield func([]T, error) bool) {
			if size <= 0 {
				var zero []T
				yield(zero, errors.New("stream: batch size must be positive"))
				return
			}
			batch := make([]T, 0, size)
			for item, err := range up {
				if err != nil {
					var zero []T
					yield(zero, err)
					return
				}
				batch = append(batch, item)
				if len(batch) == size {
					if !yield(batch, nil) {
						return
					}
					batch = make([]T, 0, size)
				}
			}
			if len(batch) > 0 {
				yield(batch, nil)
			}
		}
	}
}

// Collect materializes a bounded stream into one item. A non-positive limit
// is rejected at iteration time so the operator remains lazy.
func Collect[T any](maxItems int) StreamOp[T, []T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[[]T, error] {
		return func(yield func([]T, error) bool) {
			if maxItems <= 0 {
				var zero []T
				yield(zero, errors.New("stream: collect maxItems must be positive"))
				return
			}
			items := make([]T, 0, maxItems)
			for item, err := range up {
				if err != nil {
					var zero []T
					yield(zero, err)
					return
				}
				if len(items) == maxItems {
					var zero []T
					yield(zero, errors.New("stream: collect maxItems exceeded"))
					return
				}
				items = append(items, item)
			}
			yield(items, nil)
		}
	}
}

// Reduce consumes the complete stream and emits one accumulated value.
func Reduce[T, Acc any](initial Acc, fn func(Acc, T) (Acc, error)) StreamOp[T, Acc] {
	return func(up iter.Seq2[T, error]) iter.Seq2[Acc, error] {
		return func(yield func(Acc, error) bool) {
			acc := initial
			for item, err := range up {
				if err != nil {
					yield(acc, err)
					return
				}
				acc, err = fn(acc, item)
				if err != nil {
					yield(acc, err)
					return
				}
			}
			yield(acc, nil)
		}
	}
}

// FlatMap expands each successful item sequentially. The inner stream must
// honor the downstream yield result; no goroutine is created per item.
func FlatMap[In, Out any](fn func(In) iter.Seq2[Out, error]) StreamOp[In, Out] {
	return func(up iter.Seq2[In, error]) iter.Seq2[Out, error] {
		return func(yield func(Out, error) bool) {
			stopped := false
			for item, err := range up {
				if err != nil {
					var zero Out
					if !yield(zero, err) {
						return
					}
					continue
				}
				fn(item)(func(out Out, innerErr error) bool {
					if !yield(out, innerErr) {
						stopped = true
						return false
					}
					return true
				})
				if stopped {
					return
				}
			}
		}
	}
}

// WithContext stops an operation when ctx is canceled before or during
// iteration. It preserves upstream errors and downstream early stop.
func WithContext[T any](ctx context.Context) StreamOp[T, T] {
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			for item, err := range up {
				if ctx != nil {
					if ctxErr := ctx.Err(); ctxErr != nil {
						var zero T
						yield(zero, ctxErr)
						return
					}
				}
				if !yield(item, err) {
					return
				}
			}
		}
	}
}
