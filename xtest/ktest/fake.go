package ktest

import (
	"context"
	"sync"

	"github.com/nexssp/kernel/action"
)

// Echo returns a typed action that returns its request unchanged.
func Echo[T any](name string) *action.Builder[T, T] {
	return action.New(name, func(_ context.Context, req T) (T, error) {
		return req, nil
	})
}

// Returns returns a typed action that always returns value.
func Returns[Req, Res any](name string, value Res) *action.Builder[Req, Res] {
	return action.New(name, func(_ context.Context, _ Req) (Res, error) {
		return value, nil
	})
}

// Fails returns a typed action that always returns err and the zero response.
func Fails[Req, Res any](name string, err error) *action.Builder[Req, Res] {
	return action.New(name, func(_ context.Context, _ Req) (Res, error) {
		var zero Res
		return zero, err
	})
}

// Sequence returns values in order and repeats the final value thereafter.
// The sequence is safe for concurrent action execution.
func Sequence[Req, Res any](name string, values ...Res) *action.Builder[Req, Res] {
	if len(values) == 0 {
		panic("ktest.Sequence: at least one value required")
	}
	values = append([]Res(nil), values...)
	var mu sync.Mutex
	index := 0
	return action.New(name, func(_ context.Context, _ Req) (Res, error) {
		mu.Lock()
		value := values[index]
		if index < len(values)-1 {
			index++
		}
		mu.Unlock()
		return value, nil
	})
}

// Flaky returns err for the first failures executions and then returns value.
// It is deterministic and safe for concurrent action execution.
func Flaky[Req, Res any](name string, failures int, err error, value Res) *action.Builder[Req, Res] {
	if failures < 0 {
		panic("ktest.Flaky: failures must not be negative")
	}
	var mu sync.Mutex
	calls := 0
	return action.New(name, func(_ context.Context, _ Req) (Res, error) {
		mu.Lock()
		call := calls
		calls++
		mu.Unlock()
		if call < failures {
			var zero Res
			return zero, err
		}
		return value, nil
	})
}

// Fake returns an untyped action that always succeeds with res.
// Prefer Returns when the request and response types are known.
func Fake(name string, res any) action.AnyAction {
	return Returns[any, any](name, res).Build()
}

// FakeErr returns an untyped action that always fails with err.
// Prefer Fails when the request and response types are known.
func FakeErr(name string, err error) action.AnyAction {
	return Fails[any, any](name, err).Build()
}

// FakeSeq returns an untyped action that yields each value in order, then
// repeats the last one. Prefer Sequence when the response type is known.
func FakeSeq(name string, values ...any) action.AnyAction {
	return Sequence[any, any](name, values...).Build()
}

// NewOkAction returns a new action builder that accepts struct{} and returns ("ok", nil).
func NewOkAction(name string) *action.Builder[struct{}, string] {
	return Returns[struct{}, string](name, "ok")
}

// NewErrAction returns a new action builder that accepts struct{} and returns ("", err).
func NewErrAction(name string, err error) *action.Builder[struct{}, string] {
	return Fails[struct{}, string](name, err)
}
