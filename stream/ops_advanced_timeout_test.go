package stream_test

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/nexssp/kernel/stream"
)

func TestBatchByTimeFlushesWhileInputContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	source := iter.Seq2[int, error](func(yield func(int, error) bool) {
		for item := range 1000 {
			if !yield(item, nil) {
				return
			}
			timer := time.NewTimer(5 * time.Millisecond)
			<-timer.C
		}
	})
	batched := stream.BatchByTime[int](ctx, 100, 40*time.Millisecond)(source)
	type result struct {
		items []int
		err   error
	}
	firstBatch := make(chan result, 1)
	go func() {
		for items, err := range batched {
			firstBatch <- result{items: items, err: err}
			return
		}
	}()

	select {
	case got := <-firstBatch:
		if got.err != nil {
			t.Fatalf("BatchByTime() error = %v", got.err)
		}
		if len(got.items) == 0 || len(got.items) >= 100 {
			t.Fatalf("first batch size = %d, want a non-empty timed batch below 100", len(got.items))
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("BatchByTime did not flush while input continued")
	}
}
