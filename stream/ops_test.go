package stream_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/nexssp/kernel/stream"
)

func source[T any](items ...T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range items {
			if !yield(item, nil) {
				return
			}
		}
	}
}

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

func TestMapFilterTake(t *testing.T) {
	seq := stream.Take[int](2)(
		stream.Filter(func(v int) bool { return v%2 == 0 })(
			stream.Map(func(v int) int { return v * 2 })(source(1, 2, 3, 4)),
		),
	)
	got, err := collect(seq)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{2, 4}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestMapEStopsOnError(t *testing.T) {
	wantErr := errors.New("bad item")
	seq := stream.MapE(func(v int) (int, error) {
		if v == 2 {
			return 0, wantErr
		}
		return v, nil
	})(source(1, 2, 3))
	got, err := collect(seq)
	if !errors.Is(err, wantErr) || len(got) != 1 || got[0] != 1 {
		t.Fatalf("got items=%v err=%v", got, err)
	}
}

func TestBatchCollectReduceFlatMap(t *testing.T) {
	batches, err := collect(stream.Batch[int](2)(source(1, 2, 3)))
	if err != nil || len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Fatalf("batches=%v err=%v", batches, err)
	}
	collected, err := collect(stream.Collect[int](3)(source(1, 2, 3)))
	if err != nil || len(collected) != 1 || len(collected[0]) != 3 {
		t.Fatalf("collected=%v err=%v", collected, err)
	}
	sums, err := collect(stream.Reduce[int](0, func(acc, v int) (int, error) { return acc + v, nil })(source(1, 2, 3)))
	if err != nil || len(sums) != 1 || sums[0] != 6 {
		t.Fatalf("sums=%v err=%v", sums, err)
	}
	flat, err := collect(stream.FlatMap(func(v int) iter.Seq2[int, error] { return source(v, v*10) })(source(1, 2)))
	if err != nil || len(flat) != 4 || flat[3] != 20 {
		t.Fatalf("flat=%v err=%v", flat, err)
	}
}

func TestOperatorsHonorEarlyStop(t *testing.T) {
	seen := 0
	up := func(yield func(int, error) bool) {
		for i := range 100 {
			seen++
			if !yield(i, nil) {
				return
			}
		}
	}
	stream.Take[int](1)(up)(func(int, error) bool { return true })
	if seen != 1 {
		t.Fatalf("upstream consumed %d items, want 1", seen)
	}
}

func TestWithContextStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := collect(stream.WithContext[int](ctx)(source(1, 2)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context canceled", err)
	}
}

func BenchmarkFilterTyped(b *testing.B) {
	up := source(1, 2, 3, 4, 5, 6, 7, 8)
	op := stream.Filter(func(v int) bool { return v&1 == 0 })
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		op(up)(func(int, error) bool { return true })
	}
}

func BenchmarkMapTyped(b *testing.B) {
	up := source(1, 2, 3, 4, 5, 6, 7, 8)
	op := stream.Map(func(v int) int { return v + 1 })
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		op(up)(func(int, error) bool { return true })
	}
}
