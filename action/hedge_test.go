package action_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestHedge_PrimaryFast_NoHedgeSpawned(t *testing.T) {
	t.Parallel()

	var invocations atomic.Int32
	fast := action.New("fast", func(_ context.Context, n int) (int, error) {
		invocations.Add(1)
		return n * 2, nil
	}).Build()

	hedged := action.Hedge("hedged.fast", fast, 50*time.Millisecond).Build()

	res, err := hedged.Do(context.Background(), 21)
	ktest.RequireNoError(t, err)
	ktest.RequireEqual(t, res, 42)

	time.Sleep(70 * time.Millisecond)
	ktest.RequireEqual(t, invocations.Load(), int32(1))
}

func TestHedge_PrimarySlow_HedgeWins(t *testing.T) {
	t.Parallel()

	var invocations atomic.Int32
	flaky := action.New("flaky", func(ctx context.Context, n int) (int, error) {
		if invocations.Add(1) == 1 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return n, nil
			}
		}
		return n * 2, nil
	}).Build()

	hedged := action.Hedge("hedged.slow", flaky, 20*time.Millisecond).Build()

	start := time.Now()
	res, err := hedged.Do(context.Background(), 21)
	elapsed := time.Since(start)

	ktest.RequireNoError(t, err)
	ktest.RequireEqual(t, res, 42)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("hedge took too long: %v", elapsed)
	}
}

func TestHedge_PermanentError_NoSecondAttempt(t *testing.T) {
	t.Parallel()

	var invocations atomic.Int32
	bad := action.New("bad", func(_ context.Context, _ int) (int, error) {
		invocations.Add(1)
		return 0, xerr.BadRequest("nope")
	}).Build()

	_, err := action.Hedge("hedged.perm", bad, 50*time.Millisecond).Build().Do(context.Background(), 1)
	ktest.RequireErrorKind(t, err, xerr.KindBadRequest)
	ktest.RequireEqual(t, invocations.Load(), int32(1))
}

func TestHedge_AllFail_PreservesErrorKind(t *testing.T) {
	t.Parallel()

	var invocations atomic.Int32
	down := action.New("down", func(_ context.Context, _ int) (int, error) {
		invocations.Add(1)
		return 0, xerr.Unavailable("down")
	}).Build()

	_, err := action.Hedge("hedged.down", down, 50*time.Millisecond).Build().Do(context.Background(), 1)
	ktest.RequireErrorKind(t, err, xerr.KindUnavailable)
	ktest.RequireEqual(t, invocations.Load(), int32(2))
}

func TestHedge_WaitsForLoserBeforeReturning(t *testing.T) {
	t.Parallel()

	var calls, loserFinished atomic.Int32
	act := action.New("loser", func(ctx context.Context, n int) (int, error) {
		if calls.Add(1) == 1 {
			<-ctx.Done()
			time.Sleep(30 * time.Millisecond) // slow cleanup
			loserFinished.Store(1)
			return 0, ctx.Err()
		}
		return n, nil
	}).Build()

	_, err := action.Hedge("hedged.wait", act, 10*time.Millisecond).Build().Do(context.Background(), 1)
	ktest.RequireNoError(t, err)
	ktest.RequireEqual(t, loserFinished.Load(), int32(1))
}
