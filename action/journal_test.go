package action_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
)

// memJournal is an in-memory Journal for tests. It records writes so a
// test can assert the handler ran only once.
type memJournal struct {
	mu       sync.Mutex
	records  map[string][]byte
	gets     atomic.Int32
	putCount atomic.Int32
}

func newMemJournal() *memJournal {
	return &memJournal{records: make(map[string][]byte)}
}

func (m *memJournal) GetStep(_ context.Context, executionID, stepName string) (data []byte, ok bool, err error) {
	m.gets.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.records[executionID+"\x00"+stepName]
	return v, ok, nil
}

func (m *memJournal) RecordStep(_ context.Context, executionID, stepName string, payload []byte) error {
	m.putCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[executionID+"\x00"+stepName] = append([]byte(nil), payload...)
	return nil
}

func TestDurable_MemoizesAcrossCalls(t *testing.T) {
	t.Parallel()

	journal := newMemJournal()
	var calls atomic.Int32

	step := action.New("step", func(_ context.Context, n int) (int, error) {
		calls.Add(1)
		return n * 2, nil
	}).Durable(journal, "double").Build()

	ctx := xctx.WithExecutionID(context.Background(), "run-1")

	first, err := step.Do(ctx, 21)
	if err != nil || first != 42 {
		t.Fatalf("first call: %v / %v", first, err)
	}
	second, err := step.Do(ctx, 999) // different input, same key
	if err != nil || second != 42 {
		t.Fatalf("second call must replay memoized result: %v / %v", second, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
}

func TestDurable_DifferentExecutionIDs(t *testing.T) {
	t.Parallel()

	journal := newMemJournal()
	var calls atomic.Int32

	step := action.New("step", func(_ context.Context, n int) (int, error) {
		calls.Add(1)
		return n, nil
	}).Durable(journal, "s").Build()

	if _, err := step.Do(xctx.WithExecutionID(context.Background(), "run-a"), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := step.Do(xctx.WithExecutionID(context.Background(), "run-b"), 2); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler ran %d times, want 2 (different execution IDs)", got)
	}
}

func TestDurable_NoExecutionIDIsPassthrough(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	step := action.New("step", func(_ context.Context, n int) (int, error) {
		calls.Add(1)
		return n, nil
	}).Durable(newMemJournal(), "s").Build()

	for range 3 {
		if _, err := step.Do(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("passthrough ran %d times, want 3", got)
	}
}

// TestDurable_ConcurrentCallersShareSingleExecution proves the
// singleflight fix: without it, N concurrent callers with the same key
// would each run the handler.
func TestDurable_ConcurrentCallersShareSingleExecution(t *testing.T) {
	t.Parallel()

	journal := newMemJournal()

	gate := xtest.NewLatch()
	var calls atomic.Int32

	step := action.New("step", func(ctx context.Context, _ int) (string, error) {
		calls.Add(1)
		ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_ = gate.WaitContext(ctxTimeout)
		return "done", nil
	}).Durable(journal, "s").Build()

	ctx := xctx.WithExecutionID(context.Background(), "run-concurrent")

	const callers = 16
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			if _, err := step.Do(ctx, 0); err != nil {
				t.Errorf("caller error: %v", err)
			}
		}()
	}

	// Release the gate once every caller has had time to reach Do.
	xtest.Eventually(t, time.Second, func() bool { return calls.Load() >= 1 })
	gate.Signal()
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want exactly 1", got)
	}
	if got := journal.putCount.Load(); got != 1 {
		t.Fatalf("journal recorded %d times, want 1", got)
	}
}

func TestDurable_JournalReadErrorPropagates(t *testing.T) {
	t.Parallel()

	broken := &brokenJournal{}
	step := action.New("step", func(_ context.Context, _ int) (int, error) {
		return 0, nil
	}).Durable(broken, "s").Build()

	_, err := step.Do(xctx.WithExecutionID(context.Background(), "run"), 1)
	if xerr.KindFrom(err) != xerr.KindUnavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestDurable_StaleRecordIsOverwritten(t *testing.T) {
	t.Parallel()

	journal := newMemJournal()
	journal.records["run\x00s"] = []byte(`{"shape":"old"}`)

	step := action.New("step", func(_ context.Context, _ int) (int, error) {
		return 42, nil
	}).Durable(journal, "s").Build()

	got, err := step.Do(xctx.WithExecutionID(context.Background(), "run"), 1)
	if err != nil || got != 42 {
		t.Fatalf("expected handler run after stale record, got %v / %v", got, err)
	}

	var recorded int
	if err := json.Unmarshal(journal.records["run\x00s"], &recorded); err != nil || recorded != 42 {
		t.Fatalf("stale record not overwritten: %s", journal.records["run\x00s"])
	}
}

type brokenJournal struct{}

func (brokenJournal) GetStep(context.Context, string, string) (data []byte, ok bool, err error) {
	return nil, false, errors.New("disk on fire")
}
func (brokenJournal) RecordStep(context.Context, string, string, []byte) error { return nil }
