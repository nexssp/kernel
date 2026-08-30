package action

import (
	"context"
	"sync"
	"time"
)

type cacheItem[V any] struct {
	val V
	exp time.Time
}

type defaultMemoryKV[V any] struct {
	mu        sync.RWMutex
	data      map[string]cacheItem[V]
	ttl       time.Duration
	stopCh    chan struct{}
	closeOnce sync.Once
}

func newDefaultMemoryKV[V any](ttl time.Duration) *defaultMemoryKV[V] {
	c := &defaultMemoryKV[V]{
		data:   make(map[string]cacheItem[V]),
		ttl:    ttl,
		stopCh: make(chan struct{}),
	}
	if ttl > 0 {
		go c.janitor()
	}
	return c
}

func (c *defaultMemoryKV[V]) Get(ctx context.Context, k string) (V, bool, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}

	now := time.Now()

	c.mu.RLock()
	item, ok := c.data[k]
	c.mu.RUnlock()

	if !ok {
		return zero, false, nil
	}

	if !item.exp.IsZero() && now.After(item.exp) {
		c.mu.Lock()
		// Double-checked locking to avoid deleting fresh Set()
		if cur, ok := c.data[k]; ok && cur.exp.Equal(item.exp) {
			delete(c.data, k)
		}
		c.mu.Unlock()
		return zero, false, nil
	}

	return item.val, true, nil
}

func (c *defaultMemoryKV[V]) Set(ctx context.Context, k string, v V, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var exp time.Time
	now := time.Now()

	switch {
	case ttl > 0:
		exp = now.Add(ttl)
	case c.ttl > 0:
		exp = now.Add(c.ttl)
	default:
		// zero Time = never expires
	}

	c.mu.Lock()
	c.data[k] = cacheItem[V]{val: v, exp: exp}
	c.mu.Unlock()
	return nil
}

func (c *defaultMemoryKV[V]) Delete(ctx context.Context, k string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.data, k)
	c.mu.Unlock()
	return nil
}

func (c *defaultMemoryKV[V]) janitor() {
	tickInterval := c.ttl / 2
	if c.ttl < time.Minute {
		tickInterval = time.Minute
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.evictExpired()
		case <-c.stopCh:
			return
		}
	}
}

func (c *defaultMemoryKV[V]) evictExpired() {
	now := time.Now()
	c.mu.Lock()
	for k, v := range c.data {
		if !v.exp.IsZero() && now.After(v.exp) {
			delete(c.data, k)
		}
	}
	c.mu.Unlock()
}

// Stop terminates the background janitor goroutine. Safe to call multiple times.
func (c *defaultMemoryKV[V]) Stop() {
	c.closeOnce.Do(func() {
		close(c.stopCh)
	})
}
