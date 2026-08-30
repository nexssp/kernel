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
