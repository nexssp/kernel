package action_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

type UserReq struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type UserRes struct {
	Greeting string `json:"greeting"`
	CanVote  bool   `json:"can_vote"`
}

func TestInvoker_ZeroAllocFastPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	act := action.New("user.check", func(_ context.Context, req UserReq) (UserRes, error) {
		return UserRes{
			Greeting: "Hello " + req.Name,
			CanVote:  req.Age >= 18,
		}, nil
	}).Build()

	// 1. Invoker interface verification
	invoker, ok := any(act).(action.AnyDoer)
	if !ok {
		t.Fatal("expected act to implement Invoker automatically for free")
	}

	// 2. Direct exact type match
	res, err := invoker.DoAny(ctx, UserReq{Name: "Alice", Age: 20})
	if err != nil {
		t.Fatalf("DoAny failed: %v", err)
	}
	userRes := res.(UserRes)
	if !userRes.CanVote || userRes.Greeting != "Hello Alice" {
		t.Fatalf("unexpected result: %+v", userRes)
	}

	// 3. Pointer dereference fast path (*UserReq -> UserReq)
	resPtr, err := invoker.DoAny(ctx, &UserReq{Name: "Bob", Age: 16})
	if err != nil {
		t.Fatalf("DoAny with pointer failed: %v", err)
	}
	if resPtr.(UserRes).CanVote {
		t.Fatal("expected CanVote=false for 16-year-old")
	}

	// 4. Test universal InvokeAny helper
	resUniversal, err := action.InvokeAny(ctx, act, UserReq{Name: "Charlie", Age: 30})
	if err != nil || !resUniversal.(UserRes).CanVote {
		t.Fatalf("InvokeAny failed: res=%v, err=%v", resUniversal, err)
	}
}

func TestPipeWith_InlineTransform(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	act1 := action.New("step1", func(_ context.Context, n int) (UserReq, error) {
		return UserReq{Name: fmt.Sprintf("User-%d", n), Age: 25}, nil
	}).Build()

	act2 := action.New("step2", func(_ context.Context, greeting string) (string, error) {
		return strings.ToUpper(greeting), nil
	}).Build()

	// PipeWith extracts Greeting field without creating throwaway adapter actions
	pipeline := action.PipeWith("user_pipeline",
		act1,
		func(_ context.Context, u UserReq) (string, error) {
			return "Welcome " + u.Name, nil // 0 allocs
		},
		act2,
	).Build()

	res, err := pipeline.Do(ctx, 42)
	if err != nil {
		t.Fatalf("PipeWith failed: %v", err)
	}
	if res != "WELCOME USER-42" {
		t.Fatalf("expected 'WELCOME USER-42', got %q", res)
	}
}

func TestParallelNamed_HeterogeneousConcurrentScatterGather(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	actSec := action.New("sec_check", func(_ context.Context, code string) (bool, error) {
		return !strings.Contains(code, "DROP TABLE"), nil
	}).Build()

	actComplexity := action.New("complexity_check", func(_ context.Context, code string) (int, error) {
		return len(strings.Fields(code)), nil
	}).Build()

	parallel := action.ParallelNamed[string]("review_experts", map[string]action.AnyAction{
		"is_secure": actSec,
		"words":     actComplexity,
	}).Build()

	results, err := parallel.Do(ctx, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("ParallelNamed failed: %v", err)
	}

	if secure, ok := results["is_secure"].(bool); !ok || !secure {
		t.Fatalf("expected is_secure=true, got %v", results["is_secure"])
	}
	if words, ok := results["words"].(int); !ok || words != 4 {
		t.Fatalf("expected words=4, got %v", results["words"])
	}
}

func TestDynamic_CompositionViaNativePipe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	actA := action.New("stepA", func(_ context.Context, req struct{ Text string }) (map[string]any, error) {
		return map[string]any{"content": req.Text + " -> processed"}, nil
	}).Build()

	actB := action.New("stepB", func(_ context.Context, s string) (string, error) {
		return s + " -> finished", nil
	}).Build()

	// Compose two completely different structs dynamically using native Pipe
	pipe := action.Pipe[any, any, any]("dynamic_pipe",
		action.Dynamic(actA).Build(),
		action.Dynamic(actB).Build(),
	).Build()

	res, err := pipe.Do(ctx, map[string]any{"Text": "hello"})
	if err != nil {
		t.Fatalf("dynamic pipe failed: %v", err)
	}

	expected := "hello -> processed -> finished"
	if res != expected {
		t.Fatalf("expected %q, got %q", expected, res)
	}
}
