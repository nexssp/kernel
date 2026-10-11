package stream_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nexssp/kernel/stream"
)

func TestDistinct_DeduplicatesInOrder(t *testing.T) {
	t.Parallel()

	got, err := collect(stream.Distinct[int]()(source(1, 2, 2, 3, 1, 4, 3, 5)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []int{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDistinctBy_KeyExtraction(t *testing.T) {
	t.Parallel()

	type User struct{ ID, Name string }
	users := []User{{"u1", "Alice"}, {"u2", "Bob"}, {"u1", "Alice Clone"}, {"u3", "Charlie"}}

	op := stream.DistinctBy(func(u User) string { return u.ID })
	got, err := collect(op(source(users...)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 || got[0].Name != "Alice" || got[1].Name != "Bob" || got[2].Name != "Charlie" {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestDistinct_PropagatesError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("upstream failed")
	failing := func(yield func(int, error) bool) {
		if yield(1, nil) {
			yield(0, sentinel)
		}
	}

	_, err := collect(stream.Distinct[int]()(failing))
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected %v, got %v", sentinel, err)
	}
}

func TestDistinctBy_NilKeyFnYieldsError(t *testing.T) {
	t.Parallel()

	op := stream.DistinctBy[int, int](nil)
	if _, err := collect(op(source(1, 2))); err == nil {
		t.Fatal("expected error for nil keyFn")
	}
}

// Allocations must depend on cardinality, not input length.
func TestDistinct_AllocsIndependentOfInputLength(t *testing.T) {
	build := func(n int) []int {
		items := make([]int, n)
		for i := range items {
			items[i] = i % 10
		}
		return items
	}
	measure := func(items []int) float64 {
		seq := source(items...)
		op := stream.Distinct[int]()
		return testing.AllocsPerRun(100, func() {
			for range op(seq) {
			}
		})
	}

	small, large := measure(build(100)), measure(build(10000))
	if small != large {
		t.Fatalf("allocs scale with input: small=%.1f large=%.1f", small, large)
	}
}
