package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestMemoryFencedMutex_MonotonicFence(t *testing.T) {
	t.Parallel()

	mutex := action.NewMemoryFencedMutex()
	ctx := context.Background()

	l1, ok, err := mutex.Acquire(ctx, "resource_1", 50*time.Millisecond)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, ok)
	ktest.RequireEqual(t, l1.Fence, int64(1))

	rel, err := mutex.Release(ctx, l1)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, rel)

	l2, ok, err := mutex.Acquire(ctx, "resource_1", 50*time.Millisecond)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, ok)
	ktest.RequireEqual(t, l2.Fence, int64(2))
}

func TestMemoryFencedMutex_RejectsInvalidTTL(t *testing.T) {
	t.Parallel()

	mutex := action.NewMemoryFencedMutex()
	for _, ttl := range []time.Duration{0, -time.Second} {
		_, ok, err := mutex.Acquire(context.Background(), "k", ttl)
		ktest.RequireErrorKind(t, err, xerr.KindBadRequest)
		ktest.RequireTrue(t, !ok)
	}
}

func TestMemoryFencedMutex_ExpiredLeaseIsFencedOut(t *testing.T) {
	t.Parallel()

	mutex := action.NewMemoryFencedMutex()
	ctx := context.Background()

	old, ok, err := mutex.Acquire(ctx, "k", 20*time.Millisecond)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, ok)

	time.Sleep(40 * time.Millisecond)

	fresh, ok, err := mutex.Acquire(ctx, "k", time.Second)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, ok)
	if fresh.Fence <= old.Fence {
		t.Fatalf("fence did not advance: old=%d fresh=%d", old.Fence, fresh.Fence)
	}

	renewed, err := mutex.Renew(ctx, old, time.Second)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, !renewed)

	released, err := mutex.Release(ctx, old)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, !released)
}

func TestMemoryFencedMutex_HeldLockRejectsSecondAcquire(t *testing.T) {
	t.Parallel()

	mutex := action.NewMemoryFencedMutex()
	ctx := context.Background()

	_, ok, err := mutex.Acquire(ctx, "k", time.Second)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, ok)

	_, ok, err = mutex.Acquire(ctx, "k", time.Second)
	ktest.RequireNoError(t, err)
	ktest.RequireTrue(t, !ok)
}
