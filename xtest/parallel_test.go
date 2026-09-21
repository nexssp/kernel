package xtest

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunParallel_RunsAllWorkersBehindBarrier(t *testing.T) {
	const workers = 32
	var started atomic.Int32
	var completed atomic.Int32

	RunParallel(t, workers, func(index int) error {
		if index < 0 || index >= workers {
			return errors.New("invalid worker index")
		}
		started.Add(1)
		completed.Add(1)
		return nil
	})

	if got := started.Load(); got != workers {
		t.Fatalf("started=%d, want %d", got, workers)
	}
	if got := completed.Load(); got != workers {
		t.Fatalf("completed=%d, want %d", got, workers)
	}
}

func TestWaitForSignal(t *testing.T) {
	signal := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond)
		close(signal)
	}()

	WaitForSignal(t, signal, time.Second)
}
