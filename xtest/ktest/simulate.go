package ktest

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/nexssp/kernel/action"
)

type workerTB struct {
	testing.TB
	errs chan error
}

func (w *workerTB) Fatal(args ...any) {
	w.errs <- errors.New(fmt.Sprint(args...))
	runtime.Goexit()
}

func (w *workerTB) Fatalf(format string, args ...any) {
	w.errs <- fmt.Errorf(format, args...)
	runtime.Goexit()
}

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

	errs := make(chan error, concurrency)
	wtb := &workerTB{TB: tb, errs: errs}

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})

	for range concurrency {
		wg.Go(func() {
			<-startBarrier
			res, err := act.Do(context.Background(), req)
			assertFn(wtb, res, err)
		})
	}

	close(startBarrier)
	wg.Wait()
	close(errs)

	for err := range errs {
		tb.Fatal(err)
	}
}
