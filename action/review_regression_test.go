package action_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

type reviewSignalContext struct {
	context.Context
	once       sync.Once
	doneCalled chan struct{}
}

func (ctx *reviewSignalContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.doneCalled) })
	return ctx.Context.Done()
}

func TestReview_LeaderDeadlineMustNotFailFollowers(t *testing.T) {
	cases := []struct {
		name  string
		build func(*action.Builder[int, int]) *action.Builder[int, int]
		base  func() context.Context
	}{
		{"coalesce", func(b *action.Builder[int, int]) *action.Builder[int, int] {
			return b.Coalesce(action.NewCoalescer(), func(int) string { return "k" })
		}, context.Background},
		{"dedup", func(b *action.Builder[int, int]) *action.Builder[int, int] {
			return b.Dedup(func(int) string { return "k" })
		}, context.Background},
		{"idempotent", func(b *action.Builder[int, int]) *action.Builder[int, int] {
			return b.Idempotent()
		}, func() context.Context {
			return xctx.WithIdempotencyKey(context.Background(), "review-key")
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()

			act := test.build(action.New("review-"+test.name, func(ctx context.Context, _ int) (int, error) {
				close(started)
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-release:
					return 7, nil
				}
			})).Build()

			leaderCtx, cancelLeader := context.WithTimeout(test.base(), 50*time.Millisecond)
			defer cancelLeader()
			leaderResult := make(chan error, 1)
			go func() {
				_, err := act.Do(leaderCtx, 1)
				leaderResult <- err
			}()
			<-started

			followerBase, cancelFollower := context.WithTimeout(test.base(), 5*time.Second)
			defer cancelFollower()
			followerCtx := &reviewSignalContext{Context: followerBase, doneCalled: make(chan struct{})}
			type result struct {
				value int
				err   error
			}
			followerResult := make(chan result, 1)
			go func() {
				value, err := act.Do(followerCtx, 1)
				followerResult <- result{value: value, err: err}
			}()
			<-followerCtx.doneCalled
			<-leaderCtx.Done()
			unblock()

			if err := <-leaderResult; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("leader error = %v, want its own deadline", err)
			}
			if got := <-followerResult; got.err != nil || got.value != 7 {
				t.Fatalf("healthy follower = (%d, %v), want (7, nil)", got.value, got.err)
			}
		})
	}
}

func TestReview_IdempotencyScopeIsOrderInsensitive(t *testing.T) {
	var ran atomic.Int32
	act := action.New("scope-order", func(context.Context, int) (int, error) {
		ran.Add(1)
		return 1, nil
	}).Idempotent().Build()
	base := xctx.WithIdempotencyKey(xctx.WithUserID(xctx.WithTenantID(context.Background(), "t"), "u"), "retry-key")
	_, _ = act.Do(xctx.WithRoles(base, []string{"admin", "editor"}), 1)
	_, _ = act.Do(xctx.WithRoles(base, []string{"editor", "admin"}), 1)
	if ran.Load() != 1 {
		t.Fatalf("handler ran %d times; reordered roles must replay", ran.Load())
	}
}

func TestReview_ParallelMapDuplicateNamesReturnError(t *testing.T) {
	var a, b atomic.Int32
	x := action.New("fetch", func(context.Context, int) (string, error) { a.Add(1); return "A", nil }).Build()
	y := action.New("fetch", func(context.Context, int) (string, error) { b.Add(1); return "B", nil }).Build()
	_, err := action.ParallelMap[int]("both", x, y).Build().Do(context.Background(), 1)
	if err == nil {
		t.Fatal("ParallelMap accepted duplicate action names")
	}
	if a.Load() != 0 || b.Load() != 0 {
		t.Fatalf("duplicate configuration ran handlers (A=%d, B=%d)", a.Load(), b.Load())
	}
}

func TestReview_BranchAnyTagsDeterministic(t *testing.T) {
	mk := func(name, tag string) action.AnyAction {
		return action.New(name, func(context.Context, any) (any, error) { return nil, nil }).Tag(tag).Build()
	}
	seen := map[string]bool{}
	for range 60 {
		branch := action.BranchAny("br", map[string]action.AnyAction{
			"a": mk("a", "t1"), "b": mk("b", "t2"), "c": mk("c", "t3"), "d": mk("d", "t4"),
		}, func(context.Context, any) (string, error) { return "a", nil }).Build()
		seen[fmt.Sprint(branch.Describe().Tags)] = true
	}
	if len(seen) != 1 {
		keys := make([]string, 0, len(seen))
		for key := range seen {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		t.Fatalf("%d different tag orderings across identical builds: %v", len(seen), keys)
	}
}

func TestReview_SagaJSONHasNoPaddingEntries(t *testing.T) {
	saga := action.NewSaga[int, int]("two").
		AddStep("a", func(context.Context, int) (int, error) { return 1, nil }, nil).
		AddStep("b", func(context.Context, int) (int, error) { return 2, nil }, nil).Build()
	result, err := saga.Do(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var encoded struct {
		Steps []json.RawMessage `json:"steps"`
		Count int               `json:"steps_count"`
	}
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Steps) != encoded.Count {
		t.Fatalf("steps_count=%d but JSON has %d steps entries (%d bytes)", encoded.Count, len(encoded.Steps), len(raw))
	}
}

func TestReview_SagaJSONOverflowPreservesWireShape(t *testing.T) {
	const stepCount = action.MaxSagaSteps + 5
	builder := action.NewSaga[int, int]("many")
	for i := range stepCount {
		name, value := fmt.Sprintf("step_%02d", i), i
		builder.AddStep(name, func(context.Context, int) (int, error) { return value, nil }, nil)
	}
	result, err := builder.Build().Do(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var encoded struct {
		Steps         []action.StepResult `json:"steps"`
		Count         int                 `json:"steps_count"`
		StepsOverflow []action.StepResult `json:"steps_overflow"`
	}
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatal(err)
	}
	if len(encoded.Steps) != action.MaxSagaSteps || len(encoded.StepsOverflow) != 5 || encoded.Count != stepCount {
		t.Fatalf("steps=%d overflow=%d count=%d, want %d + 5 overflow entries and count %d", len(encoded.Steps), len(encoded.StepsOverflow), encoded.Count, action.MaxSagaSteps, stepCount)
	}
	wireSteps := append(append([]action.StepResult(nil), encoded.Steps...), encoded.StepsOverflow...)
	for i, step := range wireSteps {
		if want := fmt.Sprintf("step_%02d", i); step.Step != want {
			t.Fatalf("step %d = %q, want %q", i, step.Step, want)
		}
	}
	allSteps := result.AllSteps()
	if len(allSteps) != stepCount {
		t.Fatalf("AllSteps returned %d entries, want %d", len(allSteps), stepCount)
	}
	for i, step := range allSteps {
		if want := fmt.Sprintf("step_%02d", i); step.Step != want {
			t.Fatalf("AllSteps entry %d = %q, want %q", i, step.Step, want)
		}
	}
}

func TestReview_SagaAllStepsBoundsMalformedCounts(t *testing.T) {
	negative := action.SagaResult[int]{StepsCount: -1}
	if got := negative.AllSteps(); len(got) != 0 {
		t.Fatalf("negative step count returned %d entries, want 0", len(got))
	}

	tooMany := action.SagaResult[int]{
		StepsCount: action.MaxSagaSteps + 1,
		StepsOverflow: []action.StepResult{
			{Step: "counted"},
			{Step: "extra"},
		},
	}
	got := tooMany.AllSteps()
	if len(got) != action.MaxSagaSteps+1 {
		t.Fatalf("AllSteps returned %d entries, want %d", len(got), action.MaxSagaSteps+1)
	}
	if got[action.MaxSagaSteps].Step != "counted" {
		t.Fatalf("AllSteps included overflow step %q, want %q", got[action.MaxSagaSteps].Step, "counted")
	}
}

func TestReview_AssignRejectsFractionalFloatIntoInt(t *testing.T) {
	var value int64
	if err := action.Assign(&value, float64(1.5)); err == nil {
		t.Fatalf("float64(1.5) silently became %d", value)
	}
}

type reviewJournal struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (j *reviewJournal) GetStep(_ context.Context, executionID, stepName string) (data []byte, ok bool, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	data, ok = j.m[executionID+"/"+stepName]
	return data, ok, nil
}

func (j *reviewJournal) RecordStep(_ context.Context, executionID, stepName string, payload []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.m[executionID+"/"+stepName] = payload
	return nil
}

func TestReview_DurableEmptyStepNameFailsClosed(t *testing.T) {
	var ran atomic.Int32
	act := action.New("missing-step", func(context.Context, int) (int, error) {
		ran.Add(1)
		return 1, nil
	}).Durable(&reviewJournal{m: make(map[string][]byte)}, "").Build()
	ctx := xctx.WithExecutionID(context.Background(), "exec-1")
	if _, err := act.Do(ctx, 1); err == nil {
		t.Fatal("empty Durable step name did not return an error")
	}
	if ran.Load() != 0 {
		t.Fatalf("handler ran %d times despite invalid durable step name", ran.Load())
	}
}
