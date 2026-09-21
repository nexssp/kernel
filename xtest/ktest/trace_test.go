package ktest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest/ktest"
)

func buildTraced(t *testing.T, trace *ktest.Trace, name, out string, err error) *action.BuiltAction[string, string] {
	t.Helper()
	return action.New(name, func(_ context.Context, in string) (string, error) {
		if err != nil {
			return "", err
		}
		return out + in, nil
	}).AnyHook(trace.Hook()).Build()
}

func TestTrace_AssertSequence(t *testing.T) {
	trace := ktest.NewTrace()

	fetch := buildTraced(t, trace, "fetch", "raw:", nil)
	transform := buildTraced(t, trace, "transform", "clean:", nil)
	store := buildTraced(t, trace, "store", "stored:", nil)

	ctx := context.Background()
	r1, _ := fetch.Do(ctx, "A")
	r2, _ := transform.Do(ctx, r1)
	_, _ = store.Do(ctx, r2)

	trace.RequireSequence(t, "fetch", "transform", "store")
	trace.RequireOrder(t, "fetch", "store")
	trace.RequireAllSucceeded(t)
	trace.RequireCalled(t, "fetch")
	trace.RequireNotCalled(t, "missing")
}

func TestTrace_AssertErrorsAt(t *testing.T) {
	trace := ktest.NewTrace()

	ok := buildTraced(t, trace, "ok", "x", nil)
	bad := buildTraced(t, trace, "bad", "", errors.New("boom"))

	ctx := context.Background()
	_, _ = ok.Do(ctx, "a")
	_, _ = bad.Do(ctx, "b")

	trace.RequireErrorsAt(t, "bad")
}

func TestTrace_Count(t *testing.T) {
	trace := ktest.NewTrace()
	act := buildTraced(t, trace, "a", "x", nil)

	ctx := context.Background()
	for range 3 {
		_, _ = act.Do(ctx, "in")
	}

	if got := trace.Count("a"); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestTrace_Reset(t *testing.T) {
	trace := ktest.NewTrace()
	act := buildTraced(t, trace, "a", "x", nil)

	_, _ = act.Do(context.Background(), "in")
	trace.Reset()

	if got := len(trace.Names()); got != 0 {
		t.Fatalf("len after reset = %d", got)
	}
}
