package action_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestPipe(t *testing.T) {
	t.Parallel()

	act1 := action.New("step1", func(_ context.Context, req int) (int, error) { return req + 1, nil }).Build()
	act2 := action.New("step2", func(_ context.Context, req int) (int, error) { return req * 2, nil }).Build()

	pipe := action.Pipe("pipe", act1, act2).Build()

	res, err := pipe.Do(context.Background(), 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 6 { // (2+1)*2
		t.Fatalf("expected 6, got %d", res)
	}
}

func TestParallelNamedUsesExplicitRouteKeys(t *testing.T) {
	security := action.New("security.internal", func(_ context.Context, req string) (int, error) {
		return len(req), nil
	}).Build()
	summary := action.New("summary.internal", func(_ context.Context, req string) (string, error) {
		return "reviewed:" + req, nil
	}).Build()
	act := action.ParallelNamed[string]("review", map[string]action.AnyAction{
		"security": security,
		"summary":  summary,
	}).Build()

	results, err := act.Do(context.Background(), "diff")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := results["security"].(int); !ok || got != 4 {
		t.Fatalf("security result = %v, want 4", results["security"])
	}
	if got, ok := results["summary"].(string); !ok || got != "reviewed:diff" {
		t.Fatalf("summary result = %v, want reviewed:diff", results["summary"])
	}
	if len(results) != 2 {
		t.Fatalf("result keys = %v, want only explicit route keys", results)
	}
}

func TestParallelMapUsesDeclaredActionNames(t *testing.T) {
	fetch := action.New("user.fetch", func(_ context.Context, req string) (string, error) {
		return "user:" + req, nil
	}).Build()
	roles := action.New("roles.resolve", func(_ context.Context, _ string) ([]string, error) {
		return []string{"admin"}, nil
	}).Build()
	act := action.ParallelMap[string]("user", fetch, roles).Build()

	results, err := act.Do(context.Background(), "42")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := results["user.fetch"].(string); !ok || got != "user:42" {
		t.Fatalf("user.fetch result = %v, want user:42", results["user.fetch"])
	}
	if got, ok := results["roles.resolve"].([]string); !ok || !reflect.DeepEqual(got, []string{"admin"}) {
		t.Fatalf("roles.resolve result = %v, want [admin]", results["roles.resolve"])
	}
}

func TestBranchAnyInvokesOnlySelectedRouteWithOriginalInput(t *testing.T) {
	type request struct{ value string }
	type contextKey struct{}
	type observed struct {
		contextValue any
		input        any
	}

	input := &request{value: "payload"}
	ctx := context.WithValue(context.Background(), contextKey{}, "trace-1")
	selectedCalls, otherCalls := 0, 0
	routerSawInput := false
	selectedSawInput := false
	selected := action.New("selected", func(ctx context.Context, req any) (any, error) {
		selectedCalls++
		selectedSawInput = req == input
		return observed{contextValue: ctx.Value(contextKey{}), input: req}, nil
	}).Build()
	other := action.New("other", func(context.Context, any) (any, error) {
		otherCalls++
		return nil, nil
	}).Build()
	branch := action.BranchAny("branch", map[string]action.AnyAction{
		"selected": selected,
		"other":    other,
	}, func(ctx context.Context, req any) (string, error) {
		routerSawInput = ctx.Value(contextKey{}) == "trace-1" && req == input
		return "selected", nil
	}).Build()

	result, err := branch.Do(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := result.(observed)
	if !ok || got.contextValue != "trace-1" || got.input != input {
		t.Fatalf("result = %#v, want original request and context", result)
	}
	if !routerSawInput || !selectedSawInput || selectedCalls != 1 || otherCalls != 0 {
		t.Fatalf("router saw input = %v, selected = %v/%d, other calls = %d", routerSawInput, selectedSawInput, selectedCalls, otherCalls)
	}
}

func TestHashRouterAnyKeepsSameKeyOnSameMember(t *testing.T) {
	var memberACalls, memberBCalls int
	memberA := action.New("member-a", func(_ context.Context, req any) (any, error) {
		memberACalls++
		key, ok := req.(string)
		if !ok {
			return nil, fmt.Errorf("member-a request type %T, want string", req)
		}
		return "a:" + key, nil
	}).Build()
	memberB := action.New("member-b", func(_ context.Context, req any) (any, error) {
		memberBCalls++
		key, ok := req.(string)
		if !ok {
			return nil, fmt.Errorf("member-b request type %T, want string", req)
		}
		return "b:" + key, nil
	}).Build()
	act := action.HashRouterAny("hash", func(req any) string {
		key, ok := req.(string)
		if !ok {
			return fmt.Sprintf("unexpected:%T", req)
		}
		return key
	}, memberA, memberB).Build()

	first, err := act.Do(context.Background(), "account-7")
	if err != nil {
		t.Fatal(err)
	}
	second, err := act.Do(context.Background(), "account-7")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("same key routed to different results: %v then %v", first, second)
	}
	if memberACalls+memberBCalls != 2 || (memberACalls != 2 && memberBCalls != 2) {
		t.Fatalf("member calls = a:%d b:%d; want same single member twice", memberACalls, memberBCalls)
	}
}

func TestParallel(t *testing.T) {
	t.Parallel()

	b1 := action.New("p1", func(_ context.Context, req string) (string, error) { return req + "1", nil })
	b2 := action.New("p2", func(_ context.Context, req string) (string, error) { return req + "2", nil })

	para := action.Parallel("par", b1, b2).Build()

	res, err := para.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(res, []string{"req1", "req2"}) {
		t.Fatalf("expected [req1 req2], got %v", res)
	}
}
