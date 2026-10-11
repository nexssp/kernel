package stream

import (
	"errors"
	"iter"
)

// Distinct yields the first occurrence of each element. Memory grows with
// cardinality; bound the input upstream.
func Distinct[T comparable]() StreamOp[T, T] {
	return DistinctBy(func(v T) T { return v })
}

// DistinctBy yields the first element per key. keyFn must not be nil.
// Memory grows with key cardinality; bound the input upstream.
func DistinctBy[T any, K comparable](keyFn func(T) K) StreamOp[T, T] {
	if keyFn == nil {
		return func(_ iter.Seq2[T, error]) iter.Seq2[T, error] {
			return func(yield func(T, error) bool) {
				var zero T
				yield(zero, errors.New("stream.DistinctBy: keyFn must not be nil"))
			}
		}
	}
	return func(up iter.Seq2[T, error]) iter.Seq2[T, error] {
		return func(yield func(T, error) bool) {
			seen := make(map[K]struct{})
			for item, err := range up {
				if err != nil {
					if !yield(item, err) {
						return
					}
					continue
				}
				key := keyFn(item)
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				if !yield(item, nil) {
					return
				}
			}
		}
	}
}
