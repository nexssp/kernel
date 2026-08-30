package action

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeFencedMutex struct {
	mu       sync.Mutex
	lease    LockLease
	renewOK  bool
	released bool
}

func (m *fakeFencedMutex) Acquire(_ context.Context, key string, _ time.Duration) (LockLease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lease = LockLease{Key: key, Owner: "owner-1", Fence: 1}
	return m.lease, true, nil
}

func (m *fakeFencedMutex) Renew(_ context.Context, lease LockLease, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func TestExclusiveFenced_ReleasesExactLeaseAfterSuccess(t *testing.T) {
	t.Parallel()
	mutex := &fakeFencedMutex{renewOK: true}
	built := New("invoice.settle", func(context.Context, string) (string, error) {
		return "settled", nil
	}).ExclusiveFenced(mutex, time.Second, func(req string) string { return req }).Build()

	result, err := built.Do(context.Background(), "invoice-1")
	if err != nil || result != "settled" {
		t.Fatalf("result = %q, %v", result, err)
	}
	if !mutex.released {
		t.Fatal("successful fenced action did not release its lease")
	}
}

func TestExclusiveFenced_CancelsWhenLeaseIsLost(t *testing.T) {
	t.Parallel()
	mutex := &fakeFencedMutex{renewOK: false}
	built := New("invoice.settle", func(ctx context.Context, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}).ExclusiveFenced(mutex, 10*time.Millisecond, func(req string) string { return req }).Build()

	if _, err := built.Do(context.Background(), "invoice-2"); err == nil {
		t.Fatal("lost fenced lease unexpectedly returned success")
	}
	if !mutex.released {
		t.Fatal("lost fenced action did not attempt release")
	}
}
