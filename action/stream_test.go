package action_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestStreamAction(t *testing.T) {
	t.Parallel()
	stream := action.NewStream("paginate", func(ctx context.Context, limit int) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			for i := 1; i <= limit; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}, nil
	})

	seq, err := stream.Do(context.Background(), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got []int
	for v, err := range seq {
		if err != nil {
			t.Fatalf("unexpected item error: %v", err)
		}
		got = append(got, v)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("expected [1,2,3], got %v", got)
	}
}

func TestCollectStream(t *testing.T) {
	t.Parallel()
	stream := action.NewStream("collect", func(ctx context.Context, _ struct{}) (iter.Seq2[string, error], error) {
		return func(yield func(string, error) bool) {
			for _, s := range []string{"a", "b", "c"} {
				if !yield(s, nil) {
					return
				}
			}
		}, nil
	})

	// Use CollectStream helper
	values, err := action.CollectStream(context.Background(), stream, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(values) != 3 || values[0] != "a" || values[2] != "c" {
		t.Fatalf("expected [a,b,c], got %v", values)
	}
}

func TestStreamAction_ErrorInIterator(t *testing.T) {
	t.Parallel()
	stream := action.NewStream("errstream", func(ctx context.Context, _ any) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			yield(0, errors.New("item-process-failed"))
		}, nil
	})
	seq, err := stream.Do(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var items []int
	for v, err := range seq {
		if err != nil {
			// last received error stops iteration
			break
		}
		items = append(items, v)
	}
	if len(items) != 1 || items[0] != 1 {
		t.Fatalf("expected [1], got %v", items)
	}
}
