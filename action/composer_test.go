package action_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestPipe(t *testing.T) {
	t.Parallel()

	act1 := action.New("step1", func(ctx context.Context, req int) (int, error) { return req + 1, nil }).Build()
	act2 := action.New("step2", func(ctx context.Context, req int) (int, error) { return req * 2, nil }).Build()

	pipe := action.Pipe("pipe", act1, act2).Build()

	res, err := pipe.Do(context.Background(), 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 6 { // (2+1)*2
		t.Fatalf("expected 6, got %d", res)
	}
}

func TestParallel(t *testing.T) {
	t.Parallel()

	b1 := action.New("p1", func(ctx context.Context, req string) (string, error) { return req + "1", nil })
	b2 := action.New("p2", func(ctx context.Context, req string) (string, error) { return req + "2", nil })

	para := action.Parallel("par", b1, b2).Build()

	res, err := para.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(res, []string{"req1", "req2"}) {
		t.Fatalf("expected [req1 req2], got %v", res)
	}
}
