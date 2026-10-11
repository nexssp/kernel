package action

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/xerr"
)

const sweepPerAcquire = 16

type memoryLease struct {
	owner     string
	fence     int64
	expiresAt time.Time
}

// MemoryFencedMutex is an in-process FencedMutex with strictly monotonic
// fence tokens from one process-wide counter.
type MemoryFencedMutex struct {
	mu    sync.Mutex
	locks map[string]memoryLease
	fence atomic.Int64
}

func NewMemoryFencedMutex() *MemoryFencedMutex {
	return &MemoryFencedMutex{locks: make(map[string]memoryLease)}
}

func (m *MemoryFencedMutex) Acquire(ctx context.Context, key string, ttl time.Duration) (LockLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return LockLease{}, false, err
	}
	if key == "" {
		return LockLease{}, false, xerr.BadRequest("lock key cannot be empty")
	}
	if ttl <= 0 {
		return LockLease{}, false, xerr.BadRequest("lock ttl must be positive")
	}

	owner, err := generateOwnerID()
	if err != nil {
		return LockLease{}, false, xerr.Internal("failed to generate owner token", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.sweepLocked(now)

	if current, ok := m.locks[key]; ok && now.Before(current.expiresAt) {
		return LockLease{}, false, nil
	}

	fence := m.fence.Add(1)
	m.locks[key] = memoryLease{owner: owner, fence: fence, expiresAt: now.Add(ttl)}
	return LockLease{Key: key, Owner: owner, Fence: fence}, true, nil
}

func (m *MemoryFencedMutex) Renew(ctx context.Context, lease LockLease, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, xerr.BadRequest("lock ttl must be positive")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	current, ok := m.locks[lease.Key]
	if !ok || current.owner != lease.Owner || current.fence != lease.Fence {
		return false, nil
	}
	// Same boundary as Acquire: at expiresAt the lock is free.
	if !now.Before(current.expiresAt) {
		return false, nil
	}

	current.expiresAt = now.Add(ttl)
	m.locks[lease.Key] = current
	return true, nil
}

func (m *MemoryFencedMutex) Release(ctx context.Context, lease LockLease) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.locks[lease.Key]
	if !ok || current.owner != lease.Owner || current.fence != lease.Fence {
		return false, nil
	}
	delete(m.locks, lease.Key)
	return true, nil
}

// sweepLocked drops a bounded number of expired leases (crashed holders).
func (m *MemoryFencedMutex) sweepLocked(now time.Time) {
	n := 0
	for k, l := range m.locks {
		if !now.Before(l.expiresAt) {
			delete(m.locks, k)
		}
		n++
		if n >= sweepPerAcquire {
			return
		}
	}
}

func generateOwnerID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
