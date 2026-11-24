package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

type standardLockRecorder struct {
	acquired   bool
	tryKeys    []string
	unlockKeys []string
}

func (m *standardLockRecorder) TryLock(_ context.Context, key string, _ time.Duration) (bool, error) {
	m.tryKeys = append(m.tryKeys, key)
	return m.acquired, nil
}

func (m *standardLockRecorder) Unlock(_ context.Context, key string) error {
	m.unlockKeys = append(m.unlockKeys, key)
	return nil
}

func TestExclusiveRunsAndReleasesAfterLockAcquisition(t *testing.T) {
	mutex := &standardLockRecorder{acquired: true}
	runs := 0
	act := action.New("invoice.settle", func(_ context.Context, _ int) (string, error) {
		runs++
		return "settled", nil
	}).Exclusive(mutex, time.Minute, func(int) string { return "invoice-42" }).Build()

	result, err := act.Do(context.Background(), 1)
	if err != nil || result != "settled" {
		t.Fatalf("Do() = %q, %v; want settled, nil", result, err)
	}
	if runs != 1 {
		t.Fatalf("handler ran %d times, want once", runs)
	}
	wantKey := "invoice.settle:lock:invoice-42"
	if len(mutex.tryKeys) != 1 || mutex.tryKeys[0] != wantKey {
		t.Fatalf("TryLock keys = %v, want [%q]", mutex.tryKeys, wantKey)
	}
	if len(mutex.unlockKeys) != 1 || mutex.unlockKeys[0] != wantKey {
		t.Fatalf("Unlock keys = %v, want [%q]", mutex.unlockKeys, wantKey)
	}
}

func TestExclusiveContentionReturnsErrLockedWithoutRunningHandler(t *testing.T) {
	mutex := &standardLockRecorder{}
	runs := 0
	act := action.New("invoice.settle", func(_ context.Context, _ int) (string, error) {
		runs++
		return "settled", nil
	}).Exclusive(mutex, time.Minute, func(int) string { return "invoice-42" }).Build()

	result, err := act.Do(context.Background(), 1)
	if !errors.Is(err, action.ErrLocked) {
		t.Fatalf("Do() error = %v, want ErrLocked", err)
	}
	if result != "" || runs != 0 {
		t.Fatalf("Do() = %q, handler runs = %d; want zero result and no handler call", result, runs)
	}
	if len(mutex.unlockKeys) != 0 {
		t.Fatalf("Unlock keys = %v, want none", mutex.unlockKeys)
	}
}

func TestLeaderOnlyUsesRequestIndependentGlobalLockKey(t *testing.T) {
	mutex := &standardLockRecorder{acquired: true}
	act := action.New("cluster.sync", func(_ context.Context, req int) (int, error) {
		return req, nil
	}).LeaderOnly(mutex, time.Minute).Build()

	for _, req := range []int{1, 2} {
		result, err := act.Do(context.Background(), req)
		if err != nil || result != req {
			t.Fatalf("Do(%d) = %d, %v; want %d, nil", req, result, err, req)
		}
	}
	wantKey := "cluster.sync:lock:global_leader"
	if len(mutex.tryKeys) != 2 || mutex.tryKeys[0] != wantKey || mutex.tryKeys[1] != wantKey {
		t.Fatalf("TryLock keys = %v, want [%q %q]", mutex.tryKeys, wantKey, wantKey)
	}
}
