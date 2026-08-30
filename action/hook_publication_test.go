package action_test

import (
	"context"
	"sync"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestBuiltAction_AnyHookPublicationIsConcurrentSafe(t *testing.T) {
	act := action.New("hooks.concurrent", func(_ context.Context, req int) (int, error) {
		return req, nil
	}).Build()

	const writers = 32
	const readers = 32

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for range writers {
		go func() {
			defer wg.Done()
			<-start
			act.AddAnyHook(action.AnyHook{})
		}()
	}
	for range readers {
		go func() {
			defer wg.Done()
			<-start
			if _, err := act.Do(context.Background(), 1); err != nil {
				t.Errorf("action execution failed: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := len(act.GetAnyHooks()); got != writers {
		t.Fatalf("expected %d published hooks, got %d", writers, got)
	}
}
