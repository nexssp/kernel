package action_test

import (
	"context"
	"iter"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/stream"
)

// fixedStreamCallBudget is the ceiling for constant per-stream allocations.
//
// A streaming invocation legitimately pays a fixed cost:
//   - the wrapped yield closure capturing ctx, hooks, and the raw seq,
//   - the deferred panic-recovery closure,
//   - boxing iter.Seq2[T, error] into the iterator interface,
//   - the UseStream/Use hook slices growing once at build time.
//
// What MUST NOT happen is per-item allocation. That invariant is enforced
// by comparing small vs large payloads (allocsSmall == allocsLarge); the
// numeric ceiling here only catches accidental leaks of constant-size
// buffers per call.
const fixedStreamCallBudget = 20

// TestStreamAction_ZeroAllocPerItem proves the typed streaming loop
// (with hooks attached) does not scale allocation-wise with the number
// of elements in the stream: constant memory overhead regardless of
// whether the stream yields 1 or 100,000 items.
func TestStreamAction_ZeroAllocPerItem(t *testing.T) {
	act := action.NewStream("zeroalloc.stream", func(_ context.Context, items []int) (iter.Seq2[int, error], error) {
		return action.StreamFromSlice(items), nil
	}).UseStream(action.StreamHook[[]int, int]{
		OnStart: func(context.Context, []int, *action.Meta) {},
		OnItem:  func(context.Context, []int, int, error, *action.Meta) {},
		After:   func(context.Context, []int, error, *action.Meta) {},
	})

	ctx := context.Background()
	payloadSmall := make([]int, 10)
	payloadLarge := make([]int, 10000)

	allocsSmall := testing.AllocsPerRun(1000, func() {
		seq, err := act.Do(ctx, payloadSmall)
		if err != nil {
			t.Fatal(err)
		}
		for range seq {
			continue // drain the stream
		}
	})

	allocsLarge := testing.AllocsPerRun(1000, func() {
		seq, err := act.Do(ctx, payloadLarge)
		if err != nil {
			t.Fatal(err)
		}
		for range seq {
			continue // drain the stream
		}
	})

	// CRITERION 1 (the real invariant): no per-item allocation.
	// If per-item allocations existed, allocsLarge would be ~10k higher.
	if allocsSmall != allocsLarge {
		t.Fatalf("CRITICAL LEAK: stream allocates per-item on the heap! (Small: %.1f, Large: %.1f)",
			allocsSmall, allocsLarge)
	}

	// CRITERION 2: constant per-call overhead stays bounded.
	if allocsLarge > fixedStreamCallBudget {
		t.Fatalf("fixed allocation overhead per stream call exceeds %d (got: %.1f)",
			fixedStreamCallBudget, allocsLarge)
	}
}

// TestStreamOps_ZeroAlloc proves that a chain of streaming operators
// (Filter, Map) preserves stack purity and does not scale allocation-wise
// with the item count.
func TestStreamOps_ZeroAlloc(t *testing.T) {
	items := make([]int, 1000)

	filterOp := stream.Filter(func(v int) bool { return v%2 == 0 })
	mapOp := stream.Map(func(v int) int { return v * 2 })

	allocs := testing.AllocsPerRun(1000, func() {
		seq := action.StreamFromSlice(items)
		filtered := filterOp(seq)
		mapped := mapOp(filtered)

		for range mapped {
			continue // drain the pipeline
		}
	})

	// CRITERION 1: allocations must NOT scale with item count. If the
	// pipeline allocated per item, allocs would be >= len(items).
	if allocs >= float64(len(items)) {
		t.Fatalf("CRITICAL: stream ops allocate per-item! Result: %.1f", allocs)
	}

	// CRITERION 2: constant composition overhead stays bounded. Each
	// operator wraps the upstream iterator in a closure, so the fixed
	// cost is proportional to the number of operators, not item count.
	if allocs > fixedStreamCallBudget {
		t.Fatalf("fixed stream composition overhead exceeds %d (got: %.1f)",
			fixedStreamCallBudget, allocs)
	}
}

// TestStreamProxy_Swap_ZeroAlloc ensures hot-swapping with atomic.Value
// does not throttle the heap.
func TestStreamProxy_Swap_ZeroAlloc(t *testing.T) {
	st1 := action.NewStream("st1", func(context.Context, struct{}) (iter.Seq2[int, error], error) { return nil, nil })
	st2 := action.NewStream("st2", func(context.Context, struct{}) (iter.Seq2[int, error], error) { return nil, nil })

	p := action.NewStreamProxy(st1)

	allocs := testing.AllocsPerRun(1000, func() {
		p.Swap(st2)
		p.Swap(st1)
	})

	// Swap on atomic.Value boxes the state struct; two Swaps → up to 2.
	if allocs > 2 {
		t.Fatalf("StreamProxy.Swap allocates too much: %.1f", allocs)
	}
}
