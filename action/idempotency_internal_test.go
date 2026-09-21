package action

import (
	"fmt"
	"testing"
	"time"

	"github.com/nexssp/kernel/xtest"
)

func TestMemoryIdempotencyStore_EagerEviction(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore(10 * time.Millisecond)

	ctx := t.Context()
	entry := IdempotencyEntry{Status: 200, Body: []byte("ok"), StoredAt: time.Now()}
	store.Set(ctx, "key1", entry, 0)

	// Clean polling using our stdlib xtest tier
	xtest.Eventually(t, 3*time.Second, func() bool {
		_, found := store.Get(ctx, "key1")
		return !found
	})

	store.mu.RLock()
	_, exists := store.entries["key1"]
	store.mu.RUnlock()
	if exists {
		t.Fatal("expired key still present in map after Get")
	}

	store.Set(ctx, "key2", entry, 0)
	store.mu.RLock()
	count := len(store.entries)
	store.mu.RUnlock()
	if count != 1 {
		t.Fatalf("live entries = %d, want 1", count)
	}
}

func TestMemoryIdempotencyStore_PhantomDeletePrevention(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore(time.Hour)

	ctx := t.Context()

	// Seed a stale entry and capture its sequence number
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

	// A writer overwrites the key with a fresh entry (new sequence number)
	store.Set(ctx, "test-key", IdempotencyEntry{Status: 200, Body: []byte("fresh"), StoredAt: time.Now()}, time.Hour)

	// Simulate a late eviction attempt carrying the stale sequence number.
	// The double-check must reject it because the current entry's seq differs.
	store.mu.Lock()
	if cur, ok := store.entries["test-key"]; ok && cur.seq == staleSeq {
		delete(store.entries, "test-key")
	}
	store.mu.Unlock()

	entry, found := store.Get(ctx, "test-key")
	if !found || string(entry.Body) != "fresh" {
		t.Fatalf("phantom delete: fresh entry lost (found=%v)", found)
	}
}

func TestMemoryIdempotencyStore_CoordinatorClaimLifecycle(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore(time.Hour)

	ctx := t.Context()
	key := "coord-key"
	reqHash := "hash-1"

	// 1. Initial claim should be acquired
	claim1, err := store.Claim(ctx, key, reqHash, time.Second)
	if err != nil || claim1.State != IdempotencyClaimAcquired || claim1.Token == "" {
		t.Fatalf("expected claim acquired, got state=%v token=%q err=%v", claim1.State, claim1.Token, err)
	}

	// 2. Concurrent claim with same hash while in-progress should report InProgress
	claim2, err := store.Claim(ctx, key, reqHash, time.Second)
	if err != nil || claim2.State != IdempotencyClaimInProgress {
		t.Fatalf("expected InProgress, got state=%v err=%v", claim2.State, err)
	}

	// 3. Concurrent claim with different hash while in-progress should report Conflict
	claim3, err := store.Claim(ctx, key, "different-hash", time.Second)
	if err != nil || claim3.State != IdempotencyClaimConflict {
		t.Fatalf("expected Conflict, got state=%v err=%v", claim3.State, err)
	}

	// 4. Complete the active claim
	entry := IdempotencyEntry{Status: 200, Body: []byte("ok"), RequestHash: reqHash}
	if err := store.Complete(ctx, key, claim1.Token, entry, time.Hour); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// 5. Subsequent claim with same hash should report Completed
	claim4, err := store.Claim(ctx, key, reqHash, time.Second)
	if err != nil || claim4.State != IdempotencyClaimCompleted || string(claim4.Entry.Body) != "ok" {
		t.Fatalf("expected Completed, got state=%v entry=%+v err=%v", claim4.State, claim4.Entry, err)
	}
}

func TestMemoryIdempotencyStore_CapacityBounded(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore(time.Hour)
	store.maxCapacity = 16

	ctx := t.Context()
	for i := range 100 {
		store.Set(ctx, fmt.Sprintf("k%d", i), IdempotencyEntry{
			Status:   200,
			Body:     []byte("x"),
			StoredAt: time.Now(),
		}, time.Hour)
	}

	store.mu.RLock()
	size := len(store.entries)
	store.mu.RUnlock()

	if size > store.maxCapacity {
		t.Fatalf("entries size %d exceeds capacity %d", size, store.maxCapacity)
	}
}
