package ktest

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

// RunResult is a chainable assertion wrapper. Every terminal method is fatal.
type RunResult[Res any] struct {
	tb  testing.TB
	res Res
	err error
}

//nolint:revive // testing.TB intentionally precedes context in the fluent test API.
func Run[Req, Res any](
	tb testing.TB,
	act *action.BuiltAction[Req, Res],
	ctx context.Context,
	req Req,
) *RunResult[Res] {
	tb.Helper()
	res, err := act.Do(ctx, req)
	return &RunResult[Res]{tb: tb, res: res, err: err}
}

func (r *RunResult[Res]) NoError() *RunResult[Res] {
	r.tb.Helper()
	RequireNoError(r.tb, r.err)
	return r
}

func (r *RunResult[Res]) ErrorKind(kind xerr.Kind) *RunResult[Res] {
	r.tb.Helper()
	RequireErrorKind(r.tb, r.err, kind)
	return r
}

func (r *RunResult[Res]) IsError(target error) *RunResult[Res] {
	r.tb.Helper()
	RequireErrorIs(r.tb, r.err, target)
	return r
}

func (r *RunResult[Res]) Equals(want Res) *RunResult[Res] {
	r.tb.Helper()
	if !reflect.DeepEqual(r.res, want) {
		r.tb.Fatalf("result mismatch\nwant: %+v\ngot:  %+v", want, r.res)
	}
	return r
}

func (r *RunResult[Res]) Contains(substr string) *RunResult[Res] {
	r.tb.Helper()
	str := fmt.Sprint(r.res)
	if !strings.Contains(str, substr) {
		r.tb.Fatalf("expected result to contain %q, got: %q", substr, str)
	}
	return r
}

func (r *RunResult[Res]) Value() Res {
	return r.res
}

func (r *RunResult[Res]) Satisfies(fn func(res Res) bool, msgAndArgs ...any) *RunResult[Res] {
	r.tb.Helper()
	if fn == nil {
		r.tb.Fatal("satisfies predicate must not be nil")
	}
	if !fn(r.res) {
		msg := "result did not satisfy custom condition"
		if len(msgAndArgs) > 0 {
			format, ok := msgAndArgs[0].(string)
			if !ok {
				r.tb.Fatalf("satisfies message must start with a format string, got %T", msgAndArgs[0])
			}
			msg = fmt.Sprintf(format, msgAndArgs[1:]...)
		}
		r.tb.Fatalf("%s\n  got: %+v", msg, r.res)
	}
	return r
}

func (r *RunResult[Res]) ErrorContains(substr string) *RunResult[Res] {
	r.tb.Helper()
	RequireErrorContains(r.tb, r.err, substr)
	return r
}
