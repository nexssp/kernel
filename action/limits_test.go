package action_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

func awaitSignals[T any](t *testing.T, signals <-chan T, count int) {
	t.Helper()
	for range count {
		select {
		case <-signals:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for worker signal")
		}
	}
}

func updatePeak(peak *atomic.Int32, current int32) {
	for previous := peak.Load(); current > previous; previous = peak.Load() {
		if peak.CompareAndSwap(previous, current) {
			return
		}
	}
}

func TestFanOutWithOptions_ConfiguredLimitAndOrder(t *testing.T) {
	t.Parallel()

	const count = 5
	started := make(chan int, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var active, peak atomic.Int32
	act := action.New("fanout.options", func(_ context.Context, request int) (int, error) {
		current := active.Add(1)
		updatePeak(&peak, current)
		defer active.Add(-1)
		started <- request
		<-release
		return request * 10, nil
	}).Build()

	requests := []int{1, 2, 3, 4, 5}
	done := make(chan []action.AsyncResult[int], 1)
	go func() {
		done <- action.FanOutWithOptions(context.Background(), act, requests, action.AsyncOptions{
			MaxConcurrency: 2,
			Timeout:        time.Minute,
		})
	}()
	awaitSignals(t, started, 2)
	if got := len(started); got != 0 {
		t.Fatalf("started %d extra requests before release; want configured concurrency 2", got)
	}
	releaseAll()
	results := <-done
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak concurrency = %d, want 2", got)
	}
	for i, result := range results {
		if result.Err != nil || result.Value != requests[i]*10 {
			t.Fatalf("result[%d] = (%d, %v), want (%d, nil)", i, result.Value, result.Err, requests[i]*10)
		}
	}
}

func TestRaceWithOptions_ConfiguredLimit(t *testing.T) {
	t.Parallel()

	const count = 5
	started := make(chan int, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var active, peak atomic.Int32
	var calls atomic.Int32
	act := action.New("race.options", func(_ context.Context, request int) (int, error) {
		current := active.Add(1)
		updatePeak(&peak, current)
		defer active.Add(-1)
		calls.Add(1)
		started <- request
		<-release
		return 0, fmt.Errorf("candidate %d failed", request)
	}).Build()

	done := make(chan error, 1)
	go func() {
		_, err := action.RaceWithOptions(context.Background(), act, []int{1, 2, 3, 4, 5}, action.AsyncOptions{
			MaxConcurrency: 2,
			Timeout:        time.Minute,
		})
		done <- err
	}()
	awaitSignals(t, started, 2)
	if got := len(started); got != 0 {
		t.Fatalf("started %d extra candidates before release; want configured concurrency 2", got)
	}
	releaseAll()
	if err := <-done; err == nil {
		t.Fatal("Race returned nil error although all candidates failed")
	}
	if got := calls.Load(); got != count {
		t.Fatalf("candidate calls = %d, want %d", got, count)
	}
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak concurrency = %d, want 2", got)
	}
}

func TestAsyncOptions_TimeoutSignalsCancellation(t *testing.T) {
	t.Parallel()

	act := action.New("async.timeout", func(ctx context.Context, _ int) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}).Build()
	options := action.AsyncOptions{MaxConcurrency: 1, Timeout: time.Millisecond}

	results := action.FanOutWithOptions(context.Background(), act, []int{1}, options)
	if len(results) != 1 || !errors.Is(results[0].Err, context.DeadlineExceeded) {
		t.Fatalf("FanOut result = %+v, want one deadline error", results)
	}
	if _, err := action.RaceWithOptions(context.Background(), act, []int{1}, options); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Race error = %v, want deadline exceeded", err)
	}
}

func TestParallelWithOptions_ConfiguredLimitAndOrder(t *testing.T) {
	t.Parallel()

	const count = 4
	started := make(chan int, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var active, peak atomic.Int32
	builders := make([]*action.Builder[int, int], count)
	for i := range builders {
		value := i
		builders[i] = action.New(fmt.Sprintf("parallel.%d", value), func(context.Context, int) (int, error) {
			current := active.Add(1)
			updatePeak(&peak, current)
			defer active.Add(-1)
			started <- value
			<-release
			return value, nil
		})
	}

	parallel := action.ParallelWithOptions("parallel.options", action.AsyncOptions{
		MaxConcurrency: 2,
		Timeout:        time.Minute,
	}, builders...).Build()
	done := make(chan []int, 1)
	go func() {
		result, err := parallel.Do(context.Background(), 0)
		if err != nil {
			t.Errorf("Parallel returned an error: %v", err)
		}
		done <- result
	}()
	awaitSignals(t, started, 2)
	if got := len(started); got != 0 {
		t.Fatalf("started %d extra branches before release; want configured concurrency 2", got)
	}
	releaseAll()
	results := <-done
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak concurrency = %d, want 2", got)
	}
	for i, got := range results {
		if got != i {
			t.Fatalf("parallel result[%d] = %d, want %d", i, got, i)
		}
	}
}

func TestDurableWithOptions_BoundsUniqueExecutions(t *testing.T) {
	t.Parallel()

	const count = 4
	started := make(chan int, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var active, peak atomic.Int32
	var calls atomic.Int32
	step := action.New("durable.limit", func(_ context.Context, request int) (int, error) {
		current := active.Add(1)
		updatePeak(&peak, current)
		defer active.Add(-1)
		calls.Add(1)
		started <- request
		<-release
		return request, nil
	}).DurableWithOptions(newMemJournal(), "step", action.DurableOptions{
		MaxConcurrentExecutions: 2,
		Timeout:                 time.Minute,
	}).Build()

	type result struct {
		value int
		err   error
	}
	done := make(chan result, count)
	for request := range count {
		go func(request int) {
			ctx := xctx.WithExecutionID(context.Background(), fmt.Sprintf("run-%d", request))
			value, err := step.Do(ctx, request)
			done <- result{value: value, err: err}
		}(request)
	}
	awaitSignals(t, started, 2)
	if got := len(started); got != 0 {
		t.Fatalf("started %d extra durable executions before release; want concurrency 2", got)
	}
	releaseAll()
	for range count {
		if result := <-done; result.err != nil || result.value < 0 || result.value >= count {
			t.Fatalf("durable result = %+v, want a successful request value", result)
		}
	}
	if got := calls.Load(); got != count {
		t.Fatalf("handler calls = %d, want %d", got, count)
	}
	if got := peak.Load(); got != 2 {
		t.Fatalf("peak durable concurrency = %d, want 2", got)
	}
}

func TestDurableWithOptions_TimeoutReachesHandler(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 1)
	step := action.New("durable.timeout", func(ctx context.Context, _ int) (int, error) {
		if _, ok := ctx.Deadline(); !ok {
			return 0, errors.New("durable handler context has no deadline")
		}
		started <- struct{}{}
		<-ctx.Done()
		return 0, ctx.Err()
	}).DurableWithOptions(newMemJournal(), "step", action.DurableOptions{
		MaxConcurrentExecutions: 1,
		Timeout:                 10 * time.Millisecond,
	}).Build()
	ctx := xctx.WithExecutionID(context.Background(), "run-timeout")
	_, err := step.Do(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("durable error = %v, want deadline exceeded", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("durable handler did not start")
	}
}

func TestSagaCompensationTimeoutIsPerUndoAndContinues(t *testing.T) {
	t.Parallel()

	var order []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	saga := action.NewSaga[int, int]("saga.compensation-limit").
		CompensationTimeout(10*time.Millisecond).
		AddStep("first", func(context.Context, int) (int, error) { return 1, nil }, func(ctx context.Context, _ int) error {
			order = append(order, "undo-first")
			if ctx.Err() != nil {
				return fmt.Errorf("new compensation context was already canceled: %w", ctx.Err())
			}
			return nil
		}).
		AddStep("second", func(context.Context, int) (int, error) {
			cancel()
			return 2, nil
		}, func(ctx context.Context, _ int) error {
			order = append(order, "undo-second")
			if _, ok := ctx.Deadline(); !ok {
				return errors.New("compensation context has no deadline")
			}
			<-ctx.Done()
			return ctx.Err()
		}).
		AddStep("fail", func(context.Context, int) (int, error) { return 0, errors.New("forward step failed") }, nil).
		Build()

	result, err := saga.Do(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Saga error = %v, want context.Canceled", err)
	}
	if result.RolledBack || len(result.CompensationErrors) != 1 || !strings.Contains(result.CompensationErrors[0], context.DeadlineExceeded.Error()) {
		t.Fatalf("saga result = %+v, want one recorded compensation timeout", result)
	}
	if len(order) != 2 || order[0] != "undo-second" || order[1] != "undo-first" {
		t.Fatalf("undo order = %v, want [undo-second undo-first]", order)
	}
}

func TestSagaAnyWithOptions_CompensationTimeout(t *testing.T) {
	t.Parallel()

	completed := action.New("dynamic.completed", func(context.Context, any) (any, error) {
		return "completed", nil
	}).Build()
	undo := action.New("dynamic.undo", func(ctx context.Context, _ any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	failed := action.New("dynamic.failed", func(context.Context, any) (any, error) {
		return nil, errors.New("forward failure")
	}).Build()
	saga := action.SagaAnyWithOptions("dynamic.saga", []action.DynamicSagaStep{
		{Name: "completed", Do: completed, Undo: undo},
		{Name: "failed", Do: failed},
	}, action.SagaOptions{CompensationTimeout: time.Millisecond}).Build()
	_, err := saga.Do(context.Background(), "request")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dynamic Saga error = %v, want joined compensation deadline error", err)
	}
}

func TestParallelAnyWithOptions_ConfiguredLimit(t *testing.T) {
	t.Parallel()

	const count = 3
	started := make(chan int, count)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var active, peak atomic.Int32
	branches := make([]action.AnyAction, count)
	for i := range branches {
		value := i
		branches[i] = action.New(fmt.Sprintf("dynamic.parallel.%d", value), func(context.Context, any) (any, error) {
			current := active.Add(1)
			updatePeak(&peak, current)
			defer active.Add(-1)
			started <- value
			<-release
			return value, nil
		}).Build()
	}
	parallel := action.ParallelAnyWithOptions("dynamic.parallel", action.AsyncOptions{
		MaxConcurrency: 1,
		Timeout:        time.Minute,
	}, branches...).Build()
	type result struct {
		values map[string]any
		err    error
	}
	done := make(chan result, 1)
	go func() {
		values, err := parallel.Do(context.Background(), "request")
		done <- result{values: values, err: err}
	}()
	awaitSignals(t, started, 1)
	if got := len(started); got != 0 {
		t.Fatalf("started %d extra branches before release; want configured concurrency 1", got)
	}
	releaseAll()
	got := <-done
	if got.err != nil {
		t.Fatalf("ParallelAny returned an error: %v", got.err)
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrency = %d, want 1", got)
	}
	for i := range count {
		if got.values[fmt.Sprintf("dynamic.parallel.%d", i)] != i {
			t.Fatalf("parallel result for branch %d = %v, want %d", i, got.values[fmt.Sprintf("dynamic.parallel.%d", i)], i)
		}
	}
}

func TestRaceAnyWithOptions_ConfiguredLimitAndCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 3)
	var calls atomic.Int32
	candidates := make([]action.AnyAction, 3)
	for i := range candidates {
		candidates[i] = action.New(fmt.Sprintf("dynamic.race.%d", i), func(ctx context.Context, _ any) (any, error) {
			calls.Add(1)
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}).Build()
	}
	race := action.RaceAnyWithOptions("dynamic.race", action.AsyncOptions{
		MaxConcurrency: 2,
		Timeout:        time.Minute,
	}, candidates...).Build()
	done := make(chan error, 1)
	go func() {
		_, err := race.Do(ctx, "request")
		done <- err
	}()
	awaitSignals(t, started, 2)
	if got := len(started); got != 0 {
		t.Fatalf("started %d extra candidates before cancellation; want configured concurrency 2", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RaceAny error = %v, want context canceled", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("candidate calls = %d, want 2; queued candidate should not launch after cancellation", got)
	}
}

func TestRaceWithOptions_JoinsLosersAfterSuccess(t *testing.T) {
	t.Parallel()

	type request struct {
		candidate int
		payload   []int
	}
	winnerReady := make(chan struct{})
	loserRelease := make(chan struct{})
	loserStarted := make(chan struct{}, 1)
	loserCanceled := make(chan struct{}, 1)
	loserFinished := make(chan struct{}, 1)
	var winnerOnce, releaseOnce sync.Once
	defer winnerOnce.Do(func() { close(winnerReady) })
	defer releaseOnce.Do(func() { close(loserRelease) })

	act := action.New("race.join-success", func(ctx context.Context, req request) (int, error) {
		if req.candidate == 1 {
			<-winnerReady
			return req.payload[0], nil
		}
		loserStarted <- struct{}{}
		<-ctx.Done()
		loserCanceled <- struct{}{}
		<-loserRelease
		_ = req.payload[0]
		loserFinished <- struct{}{}
		return 0, ctx.Err()
	}).Build()
	type result struct {
		value int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := action.RaceWithOptions(context.Background(), act, []request{
			{candidate: 1, payload: []int{11}},
			{candidate: 2, payload: []int{22}},
		}, action.AsyncOptions{MaxConcurrency: 2, Timeout: time.Minute})
		done <- result{value: value, err: err}
	}()

	awaitSignals(t, loserStarted, 1)
	winnerOnce.Do(func() { close(winnerReady) })
	awaitSignals(t, loserCanceled, 1)
	select {
	case got := <-done:
		t.Fatalf("Race returned before the gated loser finished: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(loserRelease) })
	got := <-done
	if got.err != nil || got.value != 11 {
		t.Fatalf("Race result = (%d, %v), want (11, nil)", got.value, got.err)
	}
	awaitSignals(t, loserFinished, 1)
}

func TestRaceWithOptions_JoinsStartedCandidatesAfterTimeout(t *testing.T) {
	t.Parallel()

	type request struct {
		candidate int
		payload   []int
	}
	started := make(chan int, 3)
	timedOut := make(chan int, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var calls atomic.Int32
	act := action.New("race.join-timeout", func(ctx context.Context, req request) (int, error) {
		calls.Add(1)
		started <- req.candidate
		<-ctx.Done()
		timedOut <- req.candidate
		<-release
		_ = req.payload[0]
		return 0, ctx.Err()
	}).Build()
	type result struct {
		value int
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := action.RaceWithOptions(context.Background(), act, []request{
			{candidate: 1, payload: []int{11}},
			{candidate: 2, payload: []int{22}},
			{candidate: 3, payload: []int{33}},
		}, action.AsyncOptions{MaxConcurrency: 2, Timeout: time.Second})
		done <- result{value: value, err: err}
	}()

	awaitSignals(t, started, 2)
	awaitSignals(t, timedOut, 2)
	select {
	case got := <-done:
		t.Fatalf("Race returned before timed-out candidates finished: %+v", got)
	case <-time.After(25 * time.Millisecond):
	}
	releaseAll()
	got := <-done
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("Race error = %v, want deadline exceeded", got.err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("started %d candidates; want 2 because timeout must stop further launches", got)
	}
}
