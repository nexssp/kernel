package action

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/ringbuf"
)

func TestHistoryBufferSmoke(t *testing.T) {
	buf := ringbuf.NewBuffer[int](5)
	for i := range 7 {
		buf.Push(i)
	}
	got := buf.Snapshot()
	if len(got) != 5 {
		t.Fatalf("Buffer.Snapshot() len = %d, want 5", len(got))
	}
	if got[0] != 2 || got[4] != 6 {
		t.Fatalf("unexpected snapshot content: %+v", got)
	}
}

func TestBuilderWithHistoryHasMiddleware(t *testing.T) {
	b := New("hist.middleware", func(ctx context.Context, r int) (int, error) { return r, nil })
	b.WithHistory(5)

	if len(b.middlewares) != 1 {
		t.Fatalf("expected exactly 1 middleware after WithHistory, got %d", len(b.middlewares))
	}
}

func TestBuildAppliesMiddleware(t *testing.T) {
	var (
		originalCalls   int
		middlewareCalls int
	)

	b := New("middleware.check", func(ctx context.Context, r int) (int, error) {
		originalCalls++
		return r, nil
	})

	b.Use(func(next Fn[int, int]) Fn[int, int] {
		return func(ctx context.Context, r int) (int, error) {
			middlewareCalls++
			return next(ctx, r)
		}
	})

	built := b.Build()
	_, _ = built.Do(context.Background(), 1)

	if originalCalls != 1 {
		t.Fatalf("original handler should run exactly once, got %d", originalCalls)
	}
	if middlewareCalls != 1 {
		t.Fatalf("middleware should run exactly once, got %d", middlewareCalls)
	}
}
