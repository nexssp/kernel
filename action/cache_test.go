package action_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type mockKVStore struct {
	mu      sync.Mutex
	store   map[string]string
	failSet bool
	onGet   func(key string)
}

func newMockStore() *mockKVStore {
	return &mockKVStore{store: make(map[string]string)}
}

func (m *mockKVStore) Get(_ context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onGet != nil {
		m.onGet(key)
	}
	v, ok := m.store[key]
	return v, ok, nil
}

func (m *mockKVStore) Set(_ context.Context, key, value string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSet {
		return errors.New("injected set error")
	}
	m.store[key] = value
	return nil
}

func (m *mockKVStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, key)
	return nil
}

func TestCache_Hit(t *testing.T) {
	t.Parallel()
	store := newMockStore()
	if err := store.Set(context.Background(), "key1", "cached", 0); err != nil {
		t.Fatalf("failed to pre-populate cache: %v", err)
	}

	var handlerCalled bool
	var hitHookCalled bool
	var executedHookCalled bool

	act := action.New("cache.hit", func(ctx context.Context, req string) (string, error) {
		handlerCalled = true
		return "fresh", nil
	}).
		Cache(10*time.Minute, func(r string) string { return r }, store).
		HookCacheHit(func(ctx context.Context, req string, res string, meta *action.Meta) {
			hitHookCalled = true
			if res != "cached" {
				t.Errorf("expected cached value 'cached', got %q", res)
			}
		}).
		HookExecuted(func(ctx context.Context, req string, res string, meta *action.Meta) {
			executedHookCalled = true
		}).
		Build()

	res, err := act.Do(context.Background(), "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "cached" {
		t.Fatalf("expected 'cached', got %q", res)
	}
	if handlerCalled {
		t.Fatal("handler should not have been called on cache hit")
	}
	if !hitHookCalled {
		t.Fatal("HookCacheHit must be called on cache hit")
	}
	if executedHookCalled {
		t.Fatal("HookExecuted must be suppressed on cache hit")
	}
}

func TestCache_Miss(t *testing.T) {
	t.Parallel()
	store := newMockStore()
	var handlerHit bool
	var executedHookCalled bool

	act := action.New("cache.miss", func(ctx context.Context, req string) (string, error) {
		handlerHit = true
		return "computed", nil
	}).
		Cache(10*time.Minute, func(r string) string { return r }, store).
		HookCacheMiss(func(ctx context.Context, req string, meta *action.Meta) {
			if req != "missKey" {
				t.Errorf("expected req 'missKey', got %q", req)
			}
		}).
		HookExecuted(func(ctx context.Context, req string, res string, meta *action.Meta) {
			executedHookCalled = true
		}).
		Build()

	res, err := act.Do(context.Background(), "missKey")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "computed" {
		t.Fatalf("expected 'computed', got %q", res)
	}
	if !handlerHit {
		t.Fatal("handler should have been called on cache miss")
	}
	if !executedHookCalled {
		t.Fatal("HookExecuted must be called on cache miss (real execution)")
	}

	stored, ok, err := store.Get(context.Background(), "missKey")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok || stored != "computed" {
		t.Fatalf("value was not stored in cache after miss")
	}
}

func TestCache_BackFill(t *testing.T) {
	t.Parallel()
	l1 := newMockStore()
	l2 := newMockStore()
	if err := l2.Set(context.Background(), "backfill", "from_l2", 0); err != nil {
		t.Fatalf("failed to pre-populate L2: %v", err)
	}

	act := action.New("cache.backfill", func(ctx context.Context, req string) (string, error) {
		return "never_called", nil
	}).
		Cache(10*time.Minute, func(r string) string { return r }, l1, l2).
		Build()

	res, err := act.Do(context.Background(), "backfill")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != "from_l2" {
		t.Fatalf("expected 'from_l2', got %q", res)
	}

	v, ok, err := l1.Get(context.Background(), "backfill")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok || v != "from_l2" {
		t.Fatal("L1 was not backfilled")
	}
}

func TestCache_Singleflight_SingleMissNotification(t *testing.T) {
	t.Parallel()
	store := newMockStore()

	started := make(chan struct{})
	block := make(chan struct{})

	var handlerCalls atomic.Int32
	var missCount atomic.Int32
	var coalesceCount atomic.Int32

	act := action.New("cache.concurrent_miss", func(ctx context.Context, req string) (string, error) {
		handlerCalls.Add(1)
		close(started)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-block:
			return "flight_value", nil
		}
	}).
		Cache(10*time.Minute, func(r string) string { return r }, store).
		HookCacheMiss(func(ctx context.Context, req string, meta *action.Meta) {
			missCount.Add(1)
		}).
		Hook(action.Hook[string, string]{
			OnCoalesced: func(ctx context.Context, req string, meta *action.Meta) {
				coalesceCount.Add(1)
			},
		}).
		Build()

	const concurrentCallers = 10
	var wg sync.WaitGroup
	wg.Add(concurrentCallers)

	// 1. Uruchom pierwszego callera (lidera flightu)
	go func() {
		defer wg.Done()
		res, err := act.Do(context.Background(), "shared_key")
		if err != nil || res != "flight_value" {
			t.Errorf("leader failed: res=%q, err=%v", res, err)
		}
	}()

	<-started // Czekamy aż lider zablokuje się wewnątrz handlera

	// 2. Uruchom 9 współbieżnych callerów, które uderzą w trwający flight
	for i := 0; i < concurrentCallers-1; i++ {
		go func() {
			defer wg.Done()
			res, err := act.Do(context.Background(), "shared_key")
			if err != nil || res != "flight_value" {
				t.Errorf("waiter failed: res=%q, err=%v", res, err)
			}
		}()
	}

	// Dajemy wątkom wystartować i zablokować się na trwającym flighcie
	time.Sleep(20 * time.Millisecond)

	// 3. Zwalniamy blokadę handlera – flight kończy się dla wszystkich
	close(block)
	wg.Wait()

	// Twarde niezmienniki biznesowe:
	if got := handlerCalls.Load(); got != 1 {
		t.Fatalf("CRITICAL: handler executed %d times, expected exactly 1", got)
	}
	if got := missCount.Load(); got != 1 {
		t.Fatalf("CRITICAL: OnCacheMiss executed %d times, expected exactly 1", got)
	}
	if got := coalesceCount.Load(); got == 0 {
		t.Fatalf("expected at least one coalesced caller, got %d", got)
	}
}

func TestCache_Singleflight_ContextCancellation(t *testing.T) {
	t.Parallel()
	store := newMockStore()

	started := make(chan struct{})
	block := make(chan struct{})
	caller2CheckingCache := make(chan struct{})
	var getCalls atomic.Int32

	store.onGet = func(key string) {
		if getCalls.Add(1) == 2 {
			close(caller2CheckingCache)
		}
	}

	act := action.New("cache.concurrent_cancel", func(ctx context.Context, req string) (string, error) {
		close(started)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-block:
			return "computed_value", nil
		}
	}).
		Cache(10*time.Minute, func(r string) string { return r }, store).
		Build()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2 := context.Background()

	var res1, res2 string
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = act.Do(ctx1, "shared_key")
	}()

	<-started

	go func() {
		defer wg.Done()
		res2, err2 = act.Do(ctx2, "shared_key")
	}()

	<-caller2CheckingCache
	for i := 0; i < 5; i++ {
		runtime.Gosched()
	}

	cancel1()

	close(block)
	wg.Wait()

	if !errors.Is(err1, context.Canceled) {
		t.Fatalf("expected caller 1 to get context.Canceled, got: %v", err1)
	}
	if res1 != "" {
		t.Fatalf("expected caller 1 to receive zero string, got: %q", res1)
	}

	if err2 != nil {
		t.Fatalf("caller 2 failed unexpectedly: %v", err2)
	}
	if res2 != "computed_value" {
		t.Fatalf("caller 2 expected 'computed_value', got %q", res2)
	}

	cached, ok, _ := store.Get(context.Background(), "shared_key")
	if !ok || cached != "computed_value" {
		t.Fatalf("expected value to be cached in store, got %q (ok=%v)", cached, ok)
	}
}

func TestCache_Singleflight_PanicRecovery(t *testing.T) {
	t.Parallel()
	store := newMockStore()

	act := action.New("cache.panic", func(ctx context.Context, req string) (string, error) {
		panic("database crashed inside cache flight")
	}).
		Cache(10*time.Minute, func(r string) string { return r }, store).
		Build()

	res, err := act.Do(context.Background(), "panic_key")
	if res != "" {
		t.Fatalf("expected empty string on panic, got %q", res)
	}
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}

	var appErr *xerr.AppError
	if !errors.As(err, &appErr) || appErr.Kind != xerr.KindInternal {
		t.Fatalf("expected KindInternal error, got %v", err)
	}
}

func TestOnce(t *testing.T) {
	t.Parallel()
	var callCount int
	act := action.New("once.test", func(ctx context.Context, req string) (string, error) {
		callCount++
		return fmt.Sprintf("call-%d", callCount), nil
	}).Once().Build()

	var wg sync.WaitGroup
	results := make([]string, 10)
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			res, err := act.Do(context.Background(), "req")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			results[idx] = res
		}(i)
	}
	wg.Wait()

	if callCount != 1 {
		t.Fatalf("expected handler to be called exactly once, got %d", callCount)
	}
	for i, r := range results {
		if r != "call-1" {
			t.Fatalf("expected result 'call-1' at index %d, got %q", i, r)
		}
	}
}

func TestOnce_ErrorCaching(t *testing.T) {
	t.Parallel()
	failCount := 0
	act := action.New("once.err", func(ctx context.Context, req string) (string, error) {
		failCount++
		return "", fmt.Errorf("fatal %d", failCount)
	}).Once().Build()

	_, err1 := act.Do(context.Background(), "req")
	if err1 == nil || !strings.Contains(err1.Error(), "fatal 1") {
		t.Fatalf("expected error 'fatal 1', got %v", err1)
	}
	_, err2 := act.Do(context.Background(), "req")
	if err2 == nil || !strings.Contains(err2.Error(), "fatal 1") {
		t.Fatalf("error should be cached, got %v", err2)
	}
	if failCount != 1 {
		t.Fatalf("handler should have run only once, got %d", failCount)
	}
}
