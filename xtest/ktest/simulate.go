package ktest

import (
	"context"
	"sync"
	"testing"

	"github.com/nexssp/kernel/action"
)

func Simulate[Req, Res any](
	tb testing.TB,
	act *action.BuiltAction[Req, Res],
	req Req,
	concurrency int,
	assertFn func(tb testing.TB, res Res, err error),
) {
	tb.Helper()
	if concurrency <= 0 {
		tb.Fatalf("ktest.Simulate: concurrency must be > 0, got %d", concurrency)
	}
	if assertFn == nil {
		tb.Fatal("ktest.Simulate: nil assertFn")
	}

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})

	for range concurrency {
		wg.Go(func() {
			<-startBarrier
			res, err := act.Do(context.Background(), req)
			assertFn(tb, res, err)
		})
	}

	close(startBarrier)
	wg.Wait()
}
