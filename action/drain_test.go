package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestDrainSupervisor_CleanDrain(t *testing.T) {
	t.Parallel()

	supervisor := action.NewDrainSupervisor()
	barrier := make(chan struct{})

	act := action.New("slow.task", func(ctx context.Context, _ any) (any, error) {
		select {
		case <-barrier:
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return nil, errors.New("test barrier timeout")
		}
	}).
		Use(supervisor.Middleware()).
		Build()

	doneCh := make(chan error, 1)
	go func() {
		_, err := act.Do(context.Background(), nil)
		doneCh <- err
	}()
	xtest.Eventually(t, 2*time.Second, func() bool { return supervisor.InFlight() == 1 })

	drainResult := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		drainResult <- supervisor.Drain(ctx)
	}()
	xtest.Eventually(t, 2*time.Second, supervisor.Draining)

	_, err := act.Do(context.Background(), nil)
	ktest.RequireErrorKind(t, err, xerr.KindShutdown)

	close(barrier)
	ktest.RequireNoError(t, <-doneCh)
	ktest.RequireNoError(t, <-drainResult)
	ktest.RequireEqual(t, supervisor.InFlight(), int64(0))
}

func TestDrainSupervisor_DrainTimeout(t *testing.T) {
	t.Parallel()

	supervisor := action.NewDrainSupervisor()
	release := make(chan struct{})
	defer close(release)

	act := action.New("stuck.task", func(_ context.Context, _ any) (any, error) {
		<-release
		return nil, nil
	}).
		Use(supervisor.Middleware()).
		Build()

	go func() { _, _ = act.Do(context.Background(), nil) }()
	xtest.Eventually(t, 2*time.Second, func() bool { return supervisor.InFlight() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := supervisor.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestDrainSupervisor_IdleDrainReturnsImmediately(t *testing.T) {
	t.Parallel()

	supervisor := action.NewDrainSupervisor()
	ktest.RequireNoError(t, supervisor.Drain(context.Background()))
	ktest.RequireNoError(t, supervisor.Drain(context.Background())) // idempotent
}
