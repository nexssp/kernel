package action

import (
	"context"
	"testing"
	"time"
)

func TestMemoryIdempotencyStore_EagerEviction(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore(10 * time.Millisecond)
	defer store.Close()

	ctx := context.Background()
	entry := IdempotencyEntry{Status: 200, Body: []byte("ok"), StoredAt: time.Now()}
	store.Set(ctx, "key1", entry, 0)

	time.Sleep(15 * time.Millisecond) // let it expire

	if _, found := store.Get(ctx, "key1"); found {
		t.Fatal("expected expired entry to be reported as not found")
	}

	store.mu.RLock()
	_, exists := store.entries["key1"]
	store.mu.RUnlock()
	if exists {
		t.Fatal("expected expired key to be deleted from map on Get")
	}

	store.Set(ctx, "key2", entry, 0)
	store.mu.RLock()
	count := len(store.entries)
	store.mu.RUnlock()
	if count != 1 {
		t.Fatalf("expected exactly 1 live entry, got %d", count)
	}
}

func TestMemoryIdempotencyStore_PhantomDeletePrevention(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore(time.Hour)
	defer store.Close()

	ctx := context.Background()

	// 1. Setup: Inject a stale entry
	staleStoredAt := time.Now().Add(-2 * time.Hour)
	store.mu.Lock()
	store.nextSeq++
	store.entries["test-key"] = memEntry{
		IdempotencyEntry: IdempotencyEntry{Status: 200, Body: []byte("stale"), StoredAt: staleStoredAt},
		ttl:              10 * time.Millisecond,
		seq:              store.nextSeq,
	}
	staleSeq := store.nextSeq
	store.mu.Unlock()

	// 2. Writer sets a fresh entry with a new sequence number
	store.Set(ctx, "test-key", IdempotencyEntry{Status: 200, Body: []byte("fresh"), StoredAt: time.Now()}, time.Hour)

	// 3. Simulate stale eviction attempt with the old sequence
	store.mu.Lock()
	if cur, ok := store.entries["test-key"]; ok && cur.seq == staleSeq {
		delete(store.entries, "test-key") // MUST NOT EXECUTE
	}
	store.mu.Unlock()

	// 4. Verify fresh entry remains untouched
	entry, found := store.Get(ctx, "test-key")
	if !found || string(entry.Body) != "fresh" {
		t.Fatalf("phantom delete occurred: expected 'fresh' entry to persist, found=%v", found)
	}
}
