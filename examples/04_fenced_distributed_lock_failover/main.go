package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
)

// MockFencedMutex simulates a distributed lock with a monotonic fence token (e.g. etcd / Redis Redlock)
type MockFencedMutex struct {
	mu           sync.Mutex
	currentLease action.LockLease
	shouldLose   atomic.Bool
}

func (m *MockFencedMutex) Acquire(_ context.Context, key string, _ time.Duration) (action.LockLease, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentLease = action.LockLease{Key: key, Owner: "node-1", Fence: 42}
	return m.currentLease, true, nil
}

func (m *MockFencedMutex) Renew(_ context.Context, lease action.LockLease, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldLose.Load() {
		return false, nil // Simulated lease loss (e.g., network partition / GC pause)
	}
	return lease == m.currentLease, nil
}

func (m *MockFencedMutex) Release(_ context.Context, _ action.LockLease) (bool, error) {
	return true, nil
}

func main() {
	mutex := &MockFencedMutex{}

	settleInvoice := action.New("invoice.settle", func(ctx context.Context, invoiceID string) (string, error) {
		fmt.Printf("🔒 Started safe settlement for invoice %s...\n", invoiceID)

		// Simulate lease loss mid-flight
		go func() {
			time.Sleep(150 * time.Millisecond)
			fmt.Println("⚠️ [Simulation] Lock lease lost in background!")
			mutex.shouldLose.Store(true)
		}()

		select {
		case <-time.After(500 * time.Millisecond):
			return "SETTLED", nil
		case <-ctx.Done():
			fmt.Printf("🛑 Action immediately canceled: %v (Preventing split-brain corruption!)\n", ctx.Err())
			return "", ctx.Err()
		}
	}).
		ExclusiveFenced(mutex, 100*time.Millisecond, func(req string) string { return req }).
		Build()

	_, err := settleInvoice.Do(context.Background(), "INV-2026-99")
	if err != nil {
		fmt.Println("✅ Kernel successfully prevented unauthorized write after lease loss:", err)
	}
}
