package action_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

// mockKVStore implements ports.KVStore[string, string] for testing.
type mockKVStore struct {
	mu    sync.Mutex
	store map[string]string
	// simulate error on Set if needed
	failSet bool
}

func newMockStore() *mockKVStore { return &mockKVStore{store: make(map[string]string)} }

func (m *mockKVStore) Get(_ context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	// Pre-populate the cache
	if err := store.Set(context.Background(), "key1", "cached", 0); err != nil {
		t.Fatalf("failed to pre-populate cache: %v", err)
	}

	var handlerCalled bool
	act := action.New("cache.hit", func(ctx context.Context, req string) (string, error) {
		handlerCalled = true
		return "fresh", nil
	}).
		Cache(10*time.Minute, func(r string) string { return r }, store).
		HookCacheHit(func(ctx context.Context, req string, res string, meta *action.Meta) {
			// verify we were notified
			if res != "cached" {
				t.Errorf("expected cached value 'cached', got %q", res)
			}
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
}

func TestCache_Miss(t *testing.T) {
	t.Parallel()
	store := newMockStore()
	var handlerHit bool
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
	// verify that the value was stored in the cache
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
	// L1 and L2, L1 empty, L2 has value → should backfill L1
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
	// L1 should now have the value
	v, ok, err := l1.Get(context.Background(), "backfill")
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if !ok || v != "from_l2" {
		t.Fatal("L1 was not backfilled")
	}
}

func TestOnce(t *testing.T) {
	t.Parallel()
	var callCount int
	act := action.New("once.test", func(ctx context.Context, req string) (string, error) {
		callCount++
		return fmt.Sprintf("call-%d", callCount), nil
	}).Once().Build()

	// Run multiple times concurrently
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
	// all results must be the same "call-1"
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
