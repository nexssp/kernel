package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

type fakeStats struct {
	cpu float64
	gr  int
}

func (f fakeStats) CPUPercent() float64 { return f.cpu }
func (f fakeStats) Goroutines() int     { return f.gr }

func TestLoadShedding_LowPriority(t *testing.T) {
	t.Parallel()
	stats := fakeStats{cpu: 80.0, gr: 500}
	mw := action.AdaptiveLoadShedding[string, string](stats, action.LoadShedConfig{
		MaxCPU:        100,
		MaxGoroutines: 1000,
	}, action.PriorityLow)
	next := func(ctx context.Context, req string) (string, error) { return "ok", nil }
	wrapped := mw(next)

	_, err := wrapped(context.Background(), "req")
	if err == nil {
		t.Fatal("expected overload error for low priority at 80% CPU")
	}
}

func TestLoadShedding_CriticalProtected(t *testing.T) {
	t.Parallel()
	stats := fakeStats{cpu: 90.0, gr: 500}
	mw := action.AdaptiveLoadShedding[string, string](stats, action.LoadShedConfig{
		MaxCPU:        100,
		MaxGoroutines: 1000,
	}, action.PriorityCritical)
	next := func(ctx context.Context, req string) (string, error) { return "ok", nil }
	wrapped := mw(next)

	res, err := wrapped(context.Background(), "req")
	if err != nil {
		t.Fatalf("critical requests should pass at 90%% CPU, got: %v", err)
	}
	if res != "ok" {
		t.Fail()
	}
}
