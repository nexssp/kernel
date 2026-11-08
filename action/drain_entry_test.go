package action_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
)

func TestDrainSupervisor_RejectsIdempotencyReplayBeforeHooksAndCache(t *testing.T) {
	supervisor := action.NewDrainSupervisor()
	cacheStore := newMockStore()
	if err := cacheStore.Set(context.Background(), "shared-cache-key", "cached response", time.Minute); err != nil {
		t.Fatalf("prepopulate cache: %v", err)
	}

	var beforeCalls, handlerCalls, cacheHits int
	act := action.New("drain.idempotent", func(_ context.Context, req string) (string, error) {
		handlerCalls++
		return "response:" + req, nil
	}).
		HookBefore(func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
			beforeCalls++
			return ctx, nil
		}).
		Cache(time.Minute, func(string) string { return "shared-cache-key" }, cacheStore).
		HookCacheHit(func(context.Context, string, string, *action.Meta) { cacheHits++ }).
		IdempotentWithConfig(action.IdempotencyConfig{Store: action.NewMemoryIdempotencyStore(time.Hour)}).
		WithDrainSupervisor(supervisor).
		Build()

	ctx := xctx.WithIdempotencyKey(context.Background(), "drain-replay-key")
	got, err := act.Do(ctx, "first payload")
	if err != nil || got != "cached response" {
		t.Fatalf("first call = %q / %v, want cached response / nil", got, err)
	}
	if beforeCalls != 1 || handlerCalls != 0 || cacheHits != 1 {
		t.Fatalf("first call counts = before %d, handler %d, cache-hit %d; want 1/0/1", beforeCalls, handlerCalls, cacheHits)
	}
	if got := supervisor.InFlight(); got != 0 {
		t.Fatalf("in-flight after first call = %d, want 0", got)
	}

	if err := supervisor.Drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := act.Do(ctx, "first payload"); xerr.KindFrom(err) != xerr.KindShutdown {
		t.Fatalf("idempotency replay during drain error = %v, want Shutdown", err)
	}
	if beforeCalls != 1 || handlerCalls != 0 || cacheHits != 1 {
		t.Fatalf("drained replay reached hooks/cache/handler: counts = %d/%d/%d, want 1/0/1", beforeCalls, handlerCalls, cacheHits)
	}

	clone := act.CloneWithHooks()
	if _, err := clone.DoAny(ctx, "first payload"); xerr.KindFrom(err) != xerr.KindShutdown {
		t.Fatalf("cloned action during drain error = %v, want Shutdown", err)
	}
}

func TestDrainSupervisor_WaitsForActionHook(t *testing.T) {
	t.Parallel()

	supervisor := action.NewDrainSupervisor()
	hookStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var beforeCalls int

	act := action.New("drain.hook", func(_ context.Context, req string) (string, error) {
		return req, nil
	}).
		HookBefore(func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
			beforeCalls++
			hookStarted <- struct{}{}
			<-release
			return ctx, nil
		}).
		WithDrainSupervisor(supervisor).
		Build()

	doDone := make(chan error, 1)
	go func() {
		_, err := act.Do(context.Background(), "ok")
		doDone <- err
	}()
	select {
	case <-hookStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("before hook did not start")
	}
	if got := supervisor.InFlight(); got != 1 {
		t.Fatalf("in-flight during hook = %d, want 1", got)
	}

	drainDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		drainDone <- supervisor.Drain(ctx)
	}()
	xtest.Eventually(t, 2*time.Second, supervisor.Draining)

	if _, err := act.Do(context.Background(), "rejected"); xerr.KindFrom(err) != xerr.KindShutdown {
		t.Fatalf("request during drain error = %v, want Shutdown", err)
	}
	if beforeCalls != 1 {
		t.Fatalf("before hook calls during drain = %d, want 1", beforeCalls)
	}

	unblock()
	if err := <-doDone; err != nil {
		t.Fatalf("in-flight action: %v", err)
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := supervisor.InFlight(); got != 0 {
		t.Fatalf("in-flight after drain = %d, want 0", got)
	}
}
