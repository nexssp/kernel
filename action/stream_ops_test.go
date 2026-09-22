package action_test

import (
	"errors"
	"io"
	"iter"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

// ── Test helpers ─────────────────────────────────────────────────────────────

func seqOf[T any](items ...T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range items {
			if !yield(item, nil) {
				return
			}
		}
	}
}

func seqWithErr[T any](items []T, terminal error) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range items {
			if !yield(item, nil) {
				return
			}
		}
		var zero T
		yield(zero, terminal)
	}
}

func drain[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var out []T
	for item, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, item)
	}
	return out, nil
}

// ── StreamFromSlice ──────────────────────────────────────────────────────────

func TestStreamFromSlice_Basic(t *testing.T) {
	t.Parallel()
	seq := action.StreamFromSlice([]int{1, 2, 3})
	got, err := drain(seq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestStreamFromSlice_Empty(t *testing.T) {
	t.Parallel()
	got, err := drain(action.StreamFromSlice([]int{}))
	if err != nil || len(got) != 0 {
		t.Fatalf("empty slice: got %v, err %v", got, err)
	}
}

func TestStreamFromSlice_BackpressureStopsSource(t *testing.T) {
	t.Parallel()
	var produced atomic.Int32
	items := []int{1, 2, 3, 4, 5}
	seq := func(yield func(int, error) bool) {
		for _, item := range items {
			produced.Add(1)
			if !yield(item, nil) {
				return
			}
		}
	}
	// consume only first 2 items
	count := 0
	for range seq {
		count++
		if count == 2 {
			break
		}
	}
	// Because range over a func-based iterator doesn't propagate break
	// to the underlying func, this test verifies our helper honors yield(false).
	// StreamFromSlice uses yield's return value to stop.
	seq2 := action.StreamFromSlice(items)
	seen := 0
	seq2(func(_ int, _ error) bool {
		seen++
		return seen < 2
	})
	if seen != 2 {
		t.Fatalf("yield(false) did not stop StreamFromSlice: seen=%d", seen)
	}
}

// ── StreamFromFunc ───────────────────────────────────────────────────────────

func TestStreamFromFunc_NormalTerminationViaEOF(t *testing.T) {
	t.Parallel()
	items := []string{"a", "b", "c"}
	i := 0
	seq := action.StreamFromFunc(func() (string, error) {
		if i >= len(items) {
			return "", io.EOF
		}
		v := items[i]
		i++
		return v, nil
	})
	got, err := drain(seq)
	if err != nil {
		t.Fatalf("io.EOF should not surface as error, got: %v", err)
	}
	if !reflect.DeepEqual(got, items) {
		t.Fatalf("got %v, want %v", got, items)
	}
}

func TestStreamFromFunc_NonEOFErrorPropagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	i := 0
	seq := action.StreamFromFunc(func() (int, error) {
		i++
		if i == 3 {
			return 0, boom
		}
		return i, nil
	})
	got, err := drain(seq)
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got: %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("got %v, want [1 2]", got)
	}
}

func TestStreamFromFunc_BackpressureStopsPulling(t *testing.T) {
	t.Parallel()
	calls := 0
	seq := action.StreamFromFunc(func() (int, error) {
		calls++
		return calls, nil
	})
	seen := 0
	seq(func(_ int, _ error) bool {
		seen++
		return seen < 3
	})
	// The 3rd call to next() happened, but yield returned false,
	// so we should not pull a 4th time.
	if calls > 3 {
		t.Fatalf("expected at most 3 pulls, got %d", calls)
	}
}

// ── StreamFilter ─────────────────────────────────────────────────────────────

func TestStreamFilter_Basic(t *testing.T) {
	t.Parallel()
	even := func(n int) bool { return n%2 == 0 }
	op := action.StreamFilter(even)
	out := op(seqOf(1, 2, 3, 4, 5, 6))
	got, err := drain(out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{2, 4, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestStreamFilter_PropagatesUpstreamError(t *testing.T) {
	t.Parallel()
	boom := errors.New("upstream failure")
	op := action.StreamFilter(func(int) bool { return true })
	out := op(seqWithErr([]int{1, 2}, boom))
	got, err := drain(out)
	if !errors.Is(err, boom) {
		t.Fatalf("expected upstream error, got: %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("got %v, want [1 2]", got)
	}
}

func TestStreamFilter_PredicateNotCalledOnError(t *testing.T) {
	t.Parallel()
	boom := errors.New("upstream failure")
	var predCalls atomic.Int32
	op := action.StreamFilter(func(int) bool {
		predCalls.Add(1)
		return true
	})
	out := op(seqWithErr([]int{1, 2}, boom))
	_, _ = drain(out)
	// Predicate should only run for the 2 valid items, not for the error.
	if got := predCalls.Load(); got != 2 {
		t.Fatalf("predicate called %d times, want 2", got)
	}
}

func TestStreamFilter_BackpressureStopsUpstream(t *testing.T) {
	t.Parallel()
	op := action.StreamFilter(func(int) bool { return true })
	out := op(seqOf(1, 2, 3, 4, 5))
	seen := 0
	out(func(_ int, _ error) bool {
		seen++
		return seen < 2
	})
	if seen != 2 {
		t.Fatalf("yield(false) did not stop filter: seen=%d", seen)
	}
}

// ── StreamCollect ────────────────────────────────────────────────────────────

func TestStreamCollect_Empty(t *testing.T) {
	t.Parallel()
	op := action.StreamCollect[int]()
	out := op(seqOf[int]())
	got, err := drain(out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("expected single empty slice, got %v", got)
	}
}

func TestStreamCollect_Basic(t *testing.T) {
	t.Parallel()
	op := action.StreamCollect[int]()
	out := op(seqOf(1, 2, 3))
	got, err := drain(out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected single slice, got %v", got)
	}
	if !reflect.DeepEqual(got[0], []int{1, 2, 3}) {
		t.Fatalf("got %v, want [1 2 3]", got[0])
	}
}

func TestStreamCollect_PropagatesUpstreamError(t *testing.T) {
	t.Parallel()
	boom := errors.New("upstream failure")
	op := action.StreamCollect[int]()
	out := op(seqWithErr([]int{1, 2}, boom))
	_, err := drain(out)
	if !errors.Is(err, boom) {
		t.Fatalf("expected upstream error, got: %v", err)
	}
}

// ── StreamCollectN ───────────────────────────────────────────────────────────

func TestStreamCollectN_ExactlyAtLimit(t *testing.T) {
	t.Parallel()
	op := action.StreamCollectN[int](3)
	out := op(seqOf(1, 2, 3))
	got, err := drain(out)
	if err != nil {
		t.Fatalf("exact limit should succeed, got: %v", err)
	}
	if !reflect.DeepEqual(got[0], []int{1, 2, 3}) {
		t.Fatalf("got %v, want [1 2 3]", got[0])
	}
}

func TestStreamCollectN_ExceedsLimit(t *testing.T) {
	t.Parallel()
	op := action.StreamCollectN[int](3)
	out := op(seqOf(1, 2, 3, 4, 5))
	_, err := drain(out)
	if err == nil {
		t.Fatal("expected overflow error, got nil")
	}
	if xerr.KindFrom(err) != xerr.KindForbidden {
		t.Fatalf("expected KindForbidden, got %v: %v", xerr.KindFrom(err), err)
	}
	if !strings.Contains(err.Error(), "max items 3 exceeded") {
		t.Fatalf("error message lacks context: %v", err)
	}
}

func TestStreamCollectN_PanicsOnNonPositiveLimit(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{0, -1, -100} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for maxItems=%d", limit)
				}
			}()
			_ = action.StreamCollectN[int](limit)
		})
	}
}

// ── Composition: Filter -> CollectN ─────────────────────────────────────────

func TestFilterThenCollectN_Chain(t *testing.T) {
	t.Parallel()
	filter := action.StreamFilter(func(n int) bool { return n > 2 })
	collect := action.StreamCollectN[int](10)

	// Manually compose: collect(filter(upstream))
	upstream := seqOf(1, 2, 3, 4, 5)
	filtered := filter(upstream)
	collected := collect(filtered)

	got, err := drain(collected)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got[0], []int{3, 4, 5}) {
		t.Fatalf("got %v, want [3 4 5]", got[0])
	}
}

// ── Benchmarks — verify zero-alloc promise ──────────────────────────────────

// BenchmarkStreamFilter_Direct proves StreamFilter itself has zero allocs
// per item when composed directly on typed iter.Seq2.
//
// The boxing that occurs at the AnyStream boundary (typedStreamOperator.Apply)
// is NOT measured here — that's a Flow-layer concern with its own benchmark.
func BenchmarkStreamFilter_Direct(b *testing.B) {
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	pred := func(n int) bool { return n%2 == 0 }
	op := action.StreamFilter(pred)

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		upstream := action.StreamFromSlice(items)
		filtered := op(upstream)
		count := 0
		for range filtered {
			count++
		}
		_ = count
	}
}
