package action

import (
	"context"
	"time"
)

type MemoryRateLimiterTestHelper struct {
	lim *memoryRateLimiter
}

func NewMemoryRateLimiterForTest(rps float64, burst int, ttl time.Duration, maxCapacity int) *MemoryRateLimiterTestHelper {
	return &MemoryRateLimiterTestHelper{
		lim: newMemoryRateLimiterWithConfig(rps, burst, ttl, maxCapacity),
	}
}

func (h *MemoryRateLimiterTestHelper) Allow(ctx context.Context, key string) (bool, error) {
	return h.lim.Allow(ctx, key)
}

func (h *MemoryRateLimiterTestHelper) Len() int {
	h.lim.mu.Lock()
	defer h.lim.mu.Unlock()
	return len(h.lim.m)
}

func (h *MemoryRateLimiterTestHelper) Has(key string) bool {
	h.lim.mu.Lock()
	defer h.lim.mu.Unlock()
	_, ok := h.lim.m[key]
	return ok
}
