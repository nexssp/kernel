package xtest

import (
	"fmt"
	"sync"
	"testing"
)

// RunParallel starts count workers behind one barrier, waits for all of them,
// and reports the first worker error after every worker has stopped.
//
// The callback must return an error instead of calling Fatal from a worker;
// this keeps failure reporting deterministic and guarantees the parent test
// does not continue while worker goroutines are still running.
func RunParallel(tb testing.TB, count int, fn func(index int) error) {
	tb.Helper()
	if count <= 0 {
		tb.Fatalf("xtest.RunParallel: count must be positive, got %d", count)
	}
	if fn == nil {
		tb.Fatal("xtest.RunParallel: nil callback")
	}

	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	wg.Add(count)
	for index := range count {
		go func() {
			defer wg.Done()
			<-start
			if err := fn(index); err != nil {
				errs <- fmt.Errorf("worker %d: %w", index, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		tb.Fatal(err)
	}
}
