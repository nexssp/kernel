package action

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// helper to avoid flaky time.Sleep in CI/CD pipelines
func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v timeout", timeout)
}

func TestDefaultMemoryKV_BasicCRUD(t *testing.T) {
	t.Parallel()
	ctx := t.Context() // ✅ Modern Go 1.24+ test-bound context

	cache := newDefaultMemoryKV[string](5 * time.Minute)
	t.Cleanup(cache.Stop) // ✅ Idiomatic cleanup

	// 1. Get non-existent key
	val, found, err := cache.Get(ctx, "missing_key")
	if err != nil {
		t.Fatalf("unexpected error on missing key: %v", err)
	}
	if found {
		t.Errorf("expected found=false for missing key, got found=%t", found)
	}
	if val != "" {
		t.Errorf("expected zero value, got %q", val)
	}

	// 2. Set key
	err = cache.Set(ctx, "session:123", "user_data", 0)
	if err != nil {
		t.Fatalf("failed to set cache item: %v", err)
	}

	// 3. Get key
	val, found, err = cache.Get(ctx, "session:123")
	if err != nil || !found || val != "user_data" {
		t.Errorf("expected 'user_data', got val=%q, found=%t, err=%v", val, found, err)
	}

	// 4. Delete key
	err = cache.Delete(ctx, "session:123")
	if err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	_, found, _ = cache.Get(ctx, "session:123")
	if found {
		t.Error("expected key to be removed after Delete")
	}
}

func TestDefaultMemoryKV_ContextCancellation(t *testing.T) {
	t.Parallel()

	cache := newDefaultMemoryKV[string](time.Hour)
	t.Cleanup(cache.Stop)

	// Create a pre-canceled context
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// 1. Get must respect canceled context
	_, _, err := cache.Get(ctx, "key")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}

	// 2. Set must respect canceled context
	err = cache.Set(ctx, "key", "val", 0)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}

	// 3. Delete must respect canceled context
	err = cache.Delete(ctx, "key")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestDefaultMemoryKV_Expiration_Deterministic(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	cache := newDefaultMemoryKV[string](20 * time.Millisecond)
	t.Cleanup(cache.Stop)

	_ = cache.Set(ctx, "expiring_key", "payload", 0)

	// Verify it exists immediately
	_, found, _ := cache.Get(ctx, "expiring_key")
	if !found {
		t.Fatal("expected item to exist immediately after set")
	}

	// Deterministic polling (never flaky on slow CI)
	eventually(t, 200*time.Millisecond, func() bool {
		_, found, _ := cache.Get(ctx, "expiring_key")
		return !found
	})

	// Verify lazy eviction actually removed it from internal map memory
	cache.mu.RLock()
	_, existsInMap := cache.data["expiring_key"]
	cache.mu.RUnlock()

	if existsInMap {
		t.Error("expected expired key to be physically purged from map on lazy Get()")
	}
}

func TestDefaultMemoryKV_CustomTTLExpiration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Default TTL is long (1 hour), but we override it per-key
	cache := newDefaultMemoryKV[string](time.Hour)
	t.Cleanup(cache.Stop)

	_ = cache.Set(ctx, "short_lived", "data", 15*time.Millisecond)

	eventually(t, 200*time.Millisecond, func() bool {
		_, found, _ := cache.Get(ctx, "short_lived")
		return !found
	})
}

func TestDefaultMemoryKV_ZeroTTLExpiration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	// Cache with no default TTL (0)
	cache := newDefaultMemoryKV[string](0)
	t.Cleanup(cache.Stop)

	_ = cache.Set(ctx, "permanent", "persists", 0)

	// Verify item has a zero expiration time
	cache.mu.RLock()
	item := cache.data["permanent"]
	cache.mu.RUnlock()

	if !item.exp.IsZero() {
		t.Errorf("expected item.exp to be zero Time (infinite), got: %v", item.exp)
	}

	val, found, err := cache.Get(ctx, "permanent")
	if err != nil || !found || val != "persists" {
		t.Errorf("expected permanent key to be found, got: %v", val)
	}
}

func TestDefaultMemoryKV_ActiveJanitorSweep(t *testing.T) {
	t.Parallel()
	cache := newDefaultMemoryKV[string](time.Hour)
	t.Cleanup(cache.Stop)

	// Artificially inject expired keys into the map without calling Get()
	cache.mu.Lock()
	cache.data["stale_1"] = cacheItem[string]{val: "old_1", exp: time.Now().Add(-10 * time.Minute)}
	cache.data["stale_2"] = cacheItem[string]{val: "old_2", exp: time.Now().Add(-1 * time.Minute)}
	cache.data["fresh_3"] = cacheItem[string]{val: "new_3", exp: time.Now().Add(10 * time.Minute)}
	cache.mu.Unlock()

	// Trigger active background sweep directly
	cache.evictExpired()

	cache.mu.RLock()
	_, has1 := cache.data["stale_1"]
	_, has2 := cache.data["stale_2"]
	_, has3 := cache.data["fresh_3"]
	cache.mu.RUnlock()

	if has1 || has2 {
		t.Error("janitor failed to sweep expired items")
	}
	if !has3 {
		t.Error("janitor incorrectly deleted fresh item")
	}
}

func TestDefaultMemoryKV_PhantomDeletePrevention(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	cache := newDefaultMemoryKV[string](time.Hour)
	t.Cleanup(cache.Stop)

	// Setup: Inject an already-expired item
	staleExp := time.Now().Add(-time.Hour)
	cache.mu.Lock()
	cache.data["target_key"] = cacheItem[string]{val: "stale", exp: staleExp}
	cache.mu.Unlock()

	// Simulate the race:
	// A reader spots the expired item. Right before the reader gets the write-lock to delete it,
	// a writer overwrites it with fresh data.

	// 1. Writer updates the key
	freshExp := time.Now().Add(time.Hour)
	cache.mu.Lock()
	cache.data["target_key"] = cacheItem[string]{val: "fresh", exp: freshExp}
	cache.mu.Unlock()

	// 2. Now simulate what Get() would do with the stale timestamp it saw earlier
	cache.mu.Lock()
	if cur, ok := cache.data["target_key"]; ok && cur.exp.Equal(staleExp) {
		delete(cache.data, "target_key") // MUST NOT EXECUTE
	}
	cache.mu.Unlock()

	// 3. Verify fresh data was NOT deleted
	val, found, err := cache.Get(ctx, "target_key")
	if err != nil || !found || val != "fresh" {
		t.Fatalf("CRITICAL BUG: Double-check lock failed! Fresh data was deleted or lost: val=%q, found=%t", val, found)
	}
}

func TestDefaultMemoryKV_HighConcurrency(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	cache := newDefaultMemoryKV[int](50 * time.Millisecond)
	t.Cleanup(cache.Stop)

	const workers = 64
	const iterations = 500
	const keySpace = 16 // High contention over 16 keys

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})

	// Half writers, half readers
	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startBarrier // Synchronize start

			for j := range iterations {
				key := fmt.Sprintf("key_%d", (workerID*iterations+j)%keySpace)
				if workerID%2 == 0 {
					_ = cache.Set(ctx, key, j, 0)
				} else {
					_, _, _ = cache.Get(ctx, key)
				}
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	// Clean sweep post-concurrency to verify map memory is coherent
	cache.evictExpired()
}
