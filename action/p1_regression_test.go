package action_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestBuiltActionWithoutExecutorReturnsInternalError(t *testing.T) {
	act := action.New[int, string]("missing.executor", nil).Build()

	_, err := act.Do(context.Background(), 1)
	if err == nil {
		t.Fatal("nil executor returned no error")
	}
	if got := xerr.KindFrom(err); got != xerr.KindInternal {
		t.Fatalf("error kind = %v, want Internal", got)
	}
	if !strings.Contains(err.Error(), "missing.executor") || !strings.Contains(err.Error(), "no executor configured") {
		t.Fatalf("error = %q, want action name and missing-executor detail", err)
	}
}

func TestDrainSupervisorDoesNotResumeAfterDrain(t *testing.T) {
	supervisor := action.NewDrainSupervisor()
	var calls atomic.Int32
	act := action.New("drain.single-use", func(context.Context, int) (int, error) {
		calls.Add(1)
		return 1, nil
	}).WithDrainSupervisor(supervisor).Build()

	if err := supervisor.Drain(context.Background()); err != nil {
		t.Fatalf("idle drain error = %v", err)
	}
	if _, err := act.Do(context.Background(), 1); err == nil || xerr.KindFrom(err) != xerr.KindShutdown {
		t.Fatalf("execution after drain error = %v, want Shutdown", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("handler calls after drain = %d, want 0", got)
	}
}
