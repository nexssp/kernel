package action_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestBranch(t *testing.T) {
	t.Parallel()
	defaultAct := action.New("default", func(ctx context.Context, req string) (string, error) {
		return "default:" + req, nil
	})
	specialAct := action.New("special", func(ctx context.Context, req string) (string, error) {
		return "special:" + req, nil
	})

	router := func(ctx context.Context, req string) (string, error) {
		if req == "magic" {
			return "s", nil
		}
		return "d", nil
	}
	br := action.Branch("test.branch", map[string]*action.Builder[string, string]{
		"d": defaultAct,
		"s": specialAct,
	}, router).Build()

	res, err := br.Do(context.Background(), "magic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "special:magic" {
		t.Fatalf("expected 'special:magic', got %q", res)
	}

	res, err = br.Do(context.Background(), "normal")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "default:normal" {
		t.Fatalf("expected 'default:normal', got %q", res)
	}
}

func TestFirstSuccess(t *testing.T) {
	t.Parallel()
	// First action fails, second succeeds
	a1 := action.New("f1", func(ctx context.Context, req int) (int, error) {
		return 0, errors.New("failure")
	})
	a2 := action.New("f2", func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	})
	fs := action.FirstSuccess("first.success", a1, a2).Build()

	res, err := fs.Do(context.Background(), 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 6 {
		t.Fatalf("expected 6, got %d", res)
	}
}

func TestChain(t *testing.T) {
	t.Parallel()
	// Chain of transformations: add 1, then multiply by 2
	add := action.New("add1", func(ctx context.Context, n int) (int, error) { return n + 1, nil })
	mul := action.New("mul2", func(ctx context.Context, n int) (int, error) { return n * 2, nil })
	ch := action.Chain("num.transform", add, mul).Build()

	res, err := ch.Do(context.Background(), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 12 { // (5+1)*2
		t.Fatalf("expected 12, got %d", res)
	}
}
