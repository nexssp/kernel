package observe_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/observe"
	"github.com/nexssp/kernel/xerr"
)

type testSink struct {
	mu     sync.Mutex
	events []observe.Event
}

func (s *testSink) Emit(_ context.Context, event observe.Event) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *testSink) kinds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.events))
	for _, event := range s.events {
		out = append(out, event.Kind)
	}
	return out
}

func TestObserveHook_WithRetry(t *testing.T) {
	t.Parallel()

	sink := &testSink{}
	var attempts int
	act := action.New("payment.process", func(context.Context, float64) (string, error) {
		attempts++
		if attempts == 1 {
			return "", xerr.Unavailable("temporary")
		}
		return "ok", nil
	}).
		Retry(1, action.ConstantBackoff(time.Millisecond)).
		AnyHook(observe.Hook(sink)).
		Build()

	if _, err := act.Do(action.WithExecutionID(context.Background(), "exec-test"), 99.95); err != nil {
		t.Fatal(err)
	}
	kinds := sink.kinds()
	if !contains(kinds, observe.KindRetry) || !contains(kinds, observe.KindExecuted) {
		t.Fatalf("expected retry and executed events, got %v", kinds)
	}
	if contains(kinds, observe.KindError) {
		t.Fatalf("did not expect final error event, got %v", kinds)
	}
}

func TestObserveHook_WithCacheHitMiss(t *testing.T) {
	t.Parallel()

	sink := &testSink{}
	act := action.New("user.find", func(_ context.Context, id string) (string, error) {
		return "User_" + id, nil
	}).
		Cache(time.Minute, func(id string) string { return id }).
		AnyHook(observe.Hook(sink)).
		Build()

	_, _ = act.Do(context.Background(), "alice")
	_, _ = act.Do(context.Background(), "alice")
	kinds := sink.kinds()
	if !contains(kinds, observe.KindCacheMiss) || !contains(kinds, observe.KindCacheHit) {
		t.Fatalf("expected cache miss and hit, got %v", kinds)
	}
}

func TestObserveHook_PanicRecovery(t *testing.T) {
	t.Parallel()

	sink := &testSink{}
	act := action.New("panic.action", func(context.Context, string) (string, error) {
		panic("boom")
	}).AnyHook(observe.Hook(sink)).Build()

	if _, err := act.Do(context.Background(), "req"); err == nil {
		t.Fatal("expected panic recovery error")
	}
	if !contains(sink.kinds(), observe.KindError) {
		t.Fatalf("expected error event after panic, got %v", sink.kinds())
	}
}

func TestObserveHook_ContextCancellationIsNotError(t *testing.T) {
	t.Parallel()

	sink := &testSink{}
	act := action.New("cancel.action", func(ctx context.Context, _ string) (string, error) {
		return "", ctx.Err()
	}).AnyHook(observe.Hook(sink)).Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = act.Do(ctx, "req")

	if kinds := sink.kinds(); contains(kinds, observe.KindError) {
		t.Fatalf("cancellation must not emit error with current Hook contract, got %v", kinds)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
