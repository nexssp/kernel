package action

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeFencedMutex struct {
	mu        sync.Mutex
	lease     LockLease
	acquireOK bool
	renewOK   bool
	released  bool
	err       error
}

func newFakeMutex() *fakeFencedMutex {
	return &fakeFencedMutex{
		acquireOK: true,
		renewOK:   true,
	}
}

func (m *fakeFencedMutex) Acquire(_ context.Context, key string, _ time.Duration) (LockLease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return LockLease{}, false, m.err
	}
	if !m.acquireOK {
		return LockLease{}, false, nil
	}
	m.lease = LockLease{Key: key, Owner: "owner-1", Fence: 42}
	return m.lease, true, nil
}

func (m *fakeFencedMutex) Renew(_ context.Context, lease LockLease, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	return m.renewOK && lease == m.lease, nil
}

func (m *fakeFencedMutex) Release(_ context.Context, lease LockLease) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lease != m.lease {
		return false, nil
	}
	m.released = true
	return true, nil
}

func TestExclusiveFenced_SuccessAndContextPropagation(t *testing.T) {
	t.Parallel()
	mutex := newFakeMutex()

	var capturedLease LockLease
	built := New("invoice.settle", func(ctx context.Context, _ string) (string, error) {
		var ok bool
		capturedLease, ok = LeaseFromContext(ctx)
		if !ok {
			t.Fatal("lease not found in context")
		}
		return "settled", nil
	}).ExclusiveFenced(mutex, 300*time.Millisecond, func(req string) string { return req }).Build()

	result, err := built.Do(context.Background(), "inv-1")
	if err != nil || result != "settled" {
		t.Fatalf("result = %q, err = %v", result, err)
	}

	if capturedLease.Fence != 42 || capturedLease.Owner != "owner-1" {
		t.Fatalf("unexpected lease: %+v", capturedLease)
	}

	mutex.mu.Lock()
	defer mutex.mu.Unlock()
	if !mutex.released {
		t.Fatal("successful action did not release its lease")
	}
}

func TestExclusiveFenced_CancelsWhenLeaseIsLost(t *testing.T) {
	t.Parallel()
	mutex := newFakeMutex()
	mutex.renewOK = false

	built := New("invoice.settle", func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}).ExclusiveFenced(mutex, 300*time.Millisecond, func(req string) string { return req }).Build()

	_, err := built.Do(context.Background(), "inv-2")
	if err == nil {
		t.Fatal("expected error on lost lease, got nil")
	}

	mutex.mu.Lock()
	defer mutex.mu.Unlock()
	if !mutex.released {
		t.Fatal("lost lease action did not cleanup/release")
	}
}

func TestExclusiveFenced_LockContention(t *testing.T) {
	t.Parallel()
	mutex := newFakeMutex()
	mutex.acquireOK = false

	built := New("invoice.settle", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).ExclusiveFenced(mutex, 300*time.Millisecond, func(req string) string { return req }).Build()

	_, err := built.Do(context.Background(), "inv-3")
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}

func TestExclusiveFenced_ValidationErrors(t *testing.T) {
	t.Parallel()
	mutex := newFakeMutex()

	t.Run("empty key rejected", func(t *testing.T) {
		built := New("invoice.settle", func(_ context.Context, _ string) (string, error) {
			return "ok", nil
		}).ExclusiveFenced(mutex, 300*time.Millisecond, func(_ string) string { return "" }).Build()

		if _, err := built.Do(context.Background(), ""); err == nil {
			t.Fatal("expected error for empty key")
		}
	})

	t.Run("short TTL rejected", func(t *testing.T) {
		built := New("invoice.settle", func(_ context.Context, _ string) (string, error) {
			return "ok", nil
		}).ExclusiveFenced(mutex, 50*time.Millisecond, func(req string) string { return req }).Build()

		if _, err := built.Do(context.Background(), "inv-4"); err == nil {
			t.Fatal("expected error for TTL < minTTL")
		}
	})

	t.Run("nil mutex rejected", func(t *testing.T) {
		built := New("invoice.settle", func(_ context.Context, _ string) (string, error) {
			return "ok", nil
		}).ExclusiveFenced(nil, 300*time.Millisecond, func(req string) string { return req }).Build()

		if _, err := built.Do(context.Background(), "inv-5"); err == nil {
			t.Fatal("expected error for nil mutex")
		}
	})
}

func TestExclusiveFenced_PanicSafety(t *testing.T) {
	t.Parallel()
	mutex := newFakeMutex()

	built := New("invoice.settle", func(_ context.Context, _ string) (string, error) {
		panic("action handler panic")
	}).ExclusiveFenced(mutex, 300*time.Millisecond, func(req string) string { return req }).Build()

	_, err := built.Do(context.Background(), "inv-6")
	if err == nil {
		t.Fatal("expected error from recovered panic")
	}

	mutex.mu.Lock()
	defer mutex.mu.Unlock()
	if !mutex.released {
		t.Fatal("lock lease was not released when handler panicked")
	}
}

func TestLeaderOnlyFenced(t *testing.T) {
	t.Parallel()
	mutex := newFakeMutex()

	built := New("cluster.sync", func(ctx context.Context, _ string) (string, error) {
		lease, ok := LeaseFromContext(ctx)
		if !ok {
			t.Fatal("lease not found in context")
		}
		if lease.Key != "cluster.sync:lock:global_leader" {
			t.Fatalf("unexpected lock key: %s", lease.Key)
		}
		return "ok", nil
	}).LeaderOnlyFenced(mutex, 300*time.Millisecond).Build()

	if _, err := built.Do(context.Background(), ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
