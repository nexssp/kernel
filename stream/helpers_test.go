package stream_test

import "iter"

// source yields each item in order without ever producing an error.
func source[T any](items ...T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range items {
			if !yield(item, nil) {
				return
			}
		}
	}
}

// collect drains until the first error and returns what was gathered.
// Use for operators where a single error terminates the stream.
func collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var out []T
	for item, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, item)
	}
	return out, nil
}

// drain exhausts the stream, keeping successful items and per-item
// errors separate. Use for concurrent operators where one failing item
// does not terminate the stream.
func drain[T any](seq iter.Seq2[T, error]) (items []T, errs []error) {
	for item, err := range seq {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		items = append(items, item)
	}
	return items, errs
}
