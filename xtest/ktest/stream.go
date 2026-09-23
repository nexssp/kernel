package ktest

import (
	"context"
	"iter"

	"github.com/nexssp/kernel/action"
)

// StreamOf returns a StreamAction that yields items sequentially with zero allocations.
func StreamOf[T any](name string, items ...T) *action.StreamAction[struct{}, T] {
	return action.NewStream(name, func(_ context.Context, _ struct{}) (iter.Seq2[T, error], error) {
		return func(yield func(T, error) bool) {
			for _, item := range items {
				if !yield(item, nil) {
					return
				}
			}
		}, nil
	})
}

// StreamFail returns a StreamAction that immediately returns err.
func StreamFail[T any](name string, err error) *action.StreamAction[struct{}, T] {
	return action.NewStream(name, func(_ context.Context, _ struct{}) (iter.Seq2[T, error], error) {
		return nil, err
	})
}

// StreamFlaky yields items up to failAfter, then yields err.
func StreamFlaky[T any](name string, items []T, failAfter int, err error) *action.StreamAction[struct{}, T] {
	return action.NewStream(name, func(_ context.Context, _ struct{}) (iter.Seq2[T, error], error) {
		return func(yield func(T, error) bool) {
			for i, item := range items {
				if i >= failAfter {
					var zero T
					yield(zero, err)
					return
				}
				if !yield(item, nil) {
					return
				}
			}
		}, nil
	})
}
