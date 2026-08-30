package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestMemoryIdempotencyStore(t *testing.T) {
	t.Parallel()

	store := action.NewMemoryIdempotencyStore(10 * time.Millisecond)
	defer store.Close()

	ctx := context.Background()
	entry := action.IdempotencyEntry{
		Status:   200,
		Body:     []byte("success"),
		StoredAt: time.Now(),
	}

	store.Set(ctx, "key-1", entry, 0)

	// Should retrieve
	ret, found := store.Get(ctx, "key-1")
	if !found || string(ret.Body) != "success" {
		t.Fatalf("expected to find entry")
	}

	// Wait for TTL expiration
	time.Sleep(15 * time.Millisecond)

	// Should be evicted/expired
	_, found = store.Get(ctx, "key-1")
	if found {
		t.Fatalf("expected entry to be expired")
	}
}
