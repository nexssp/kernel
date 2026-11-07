package action

import (
	"bytes"
	"encoding/json"
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
		Status:   200,
		Body:     []byte("stale"),
		StoredAt: staleStoredAt,
		ttl:      10 * time.Millisecond,
		seq:      store.nextSeq,
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

func TestMemoryIdempotencyStore_ExpiredClaimCanBeTakenOver(t *testing.T) {
	store := NewMemoryIdempotencyStore(time.Hour)
	ctx := t.Context()
	store.mu.Lock()
	store.claims["expired-key"] = memClaim{
		token:       "expired-token",
		requestHash: "same-request",
		expiresAt:   time.Now().Add(-time.Second),
	}
	store.mu.Unlock()

	claim, err := store.Claim(ctx, "expired-key", "same-request", time.Hour)
	if err != nil || claim.State != IdempotencyClaimAcquired || claim.Token == "" || claim.Token == "expired-token" {
		t.Fatalf("expired claim takeover = (%+v, %v)", claim, err)
	}
	entry := IdempotencyEntry{Status: 200, Body: []byte("ok"), RequestHash: "same-request"}
	if err := store.Complete(ctx, "expired-key", "expired-token", entry, time.Hour); err == nil {
		t.Fatal("expired token completed the replacement claim")
	}
	if err := store.Complete(ctx, "expired-key", claim.Token, entry, time.Hour); err != nil {
		t.Fatalf("replacement token failed to complete: %v", err)
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

func TestRequestHashConflict_LegacyAndRawDigest(t *testing.T) {
	digest, err := hashPayloadBytes("payload")
	if err != nil {
		t.Fatal(err)
	}
	legacyEntry := IdempotencyEntry{RequestHash: hashPayloadString(digest)}
	if requestHashConflict(legacyEntry, digest) {
		t.Fatal("matching legacy string hash was rejected")
	}
	different, err := hashPayloadBytes("different")
	if err != nil {
		t.Fatal(err)
	}
	if !requestHashConflict(legacyEntry, different) {
		t.Fatal("different legacy string hash was accepted")
	}
	if requestHashConflict(IdempotencyEntry{RequestHashBytes: digest}, digest) {
		t.Fatal("matching raw digest without the string field was rejected")
	}
}

func TestHashPayloadBytesRejectsUnsupportedValues(t *testing.T) {
	if _, err := hashPayloadBytes(func() {}); err == nil {
		t.Fatal("expected an error for an unsupported request payload")
	}
	if digest, err := hashPayloadBytes(nil); err != nil || digest == ([32]byte{}) {
		t.Fatalf("nil payload hash = (%x, %v), want a non-zero digest and no error", digest, err)
	}
}

type collidingTextKey struct {
	ID int
}

func (collidingTextKey) MarshalText() ([]byte, error) {
	return []byte("same"), nil
}

type textMarshalerContainer struct {
	Values map[collidingTextKey]string `json:"values"`
}

func (textMarshalerContainer) MarshalText() ([]byte, error) {
	return []byte("ignored by json.Marshal for values"), nil
}

type recursiveTextMapPayload struct {
	Next   *recursiveTextMapPayload
	Values map[collidingTextKey]string
}

type jsonMarshalerContainer struct {
	Values map[collidingTextKey]string
}

func (jsonMarshalerContainer) MarshalJSON() ([]byte, error) {
	return []byte(`{"values":"custom"}`), nil
}

func TestHashPayloadBytesRejectsTextMarshalerMapKeys(t *testing.T) {
	t.Parallel()
	badMap := map[collidingTextKey]string{{ID: 1}: "same-value"}
	cycle := &recursiveTextMapPayload{Values: badMap}
	cycle.Next = cycle
	payloads := []any{
		badMap,
		struct {
			Values any `json:"values"`
		}{Values: badMap},
		&struct {
			Values map[collidingTextKey]string `json:"values"`
		}{Values: badMap},
		[]map[collidingTextKey]string{badMap},
		[1]map[collidingTextKey]string{badMap},
		map[string]any{"nested": badMap},
		cycle,
		textMarshalerContainer{Values: badMap},
	}
	for _, payload := range payloads {
		if _, err := hashPayloadBytes(payload); err == nil {
			t.Fatalf("hashPayloadBytes(%T) accepted an ambiguous map key", payload)
		}
	}
	first, err := json.Marshal(textMarshalerContainer{Values: map[collidingTextKey]string{{ID: 1}: "same-value"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(textMarshalerContainer{Values: map[collidingTextKey]string{{ID: 2}: "same-value"}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("test payloads do not collide: %s != %s", first, second)
	}
	if _, err := hashPayloadBytes(map[string]int{"safe": 1}); err != nil {
		t.Fatalf("ordinary string map was rejected: %v", err)
	}
	if _, err := hashPayloadBytes(jsonMarshalerContainer{Values: badMap}); err != nil {
		t.Fatalf("custom JSON marshaler was rejected: %v", err)
	}
}
