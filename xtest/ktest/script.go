package ktest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/nexssp/kernel/action"
)

type Result[Res any] struct {
	Value Res
	Err   error
}

func Success[Res any](value Res) Result[Res] { return Result[Res]{Value: value} }

func Failure[Res any](err error) Result[Res] { return Result[Res]{Err: err} }

func Script[Req, Res any](results ...Result[Res]) action.Fn[Req, Res] {
	if len(results) == 0 {
		panic("ktest.Script requires at least one result")
	}
	var mu sync.Mutex
	index := 0
	return func(context.Context, Req) (Res, error) {
		mu.Lock()
		result := results[index]
		if index < len(results)-1 {
			index++
		}
		mu.Unlock()
		return result.Value, result.Err
	}
}

func MustExecute[Req, Res any](tb testing.TB, fn action.Fn[Req, Res], req Req) Res {
	tb.Helper()
	res, err := fn(context.Background(), req)
	if err != nil {
		tb.Fatalf("unexpected action error: %v", err)
	}
	return res
}

func MustError[Req, Res any](tb testing.TB, fn action.Fn[Req, Res], req Req, want error) {
	tb.Helper()
	_, err := fn(context.Background(), req)
	if !errors.Is(err, want) {
		tb.Fatalf("expected error %v, got %v", want, err)
	}
}
