package action

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
	"golang.org/x/time/rate"
)

const (
	DefaultRateLimiterCapacity = 65536
	DefaultRateLimiterTTL      = 3 * time.Minute
	maxEvictPerCall            = 16
)

type RateLimiter interface {
	Allow(ctx context.Context, key string) (bool, error)
}

func (b *Builder[Req, Res]) RateLimit(requestsPerSecond float64, burst int) *Builder[Req, Res] {
	if burst < 1 {
		burst = 1
	}
	b.meta.RateLimit = fmt.Sprintf("%.1frps (burst %d)", requestsPerSecond, burst)
	limiter := rate.NewLimiter(rate.Limit(requestsPerSecond), burst)

	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if ctx != nil && ctx.Err() != nil {
				var zero Res
				return zero, ctx.Err()
			}
			if !limiter.Allow() {
				var zero Res
				return zero, xerr.TooManyRequests("rate limit exceeded")
			}
			return next(ctx, req)
		}
	})
}

func (b *Builder[Req, Res]) RateLimitWithKey(rps float64, burst int, keyFn func(context.Context) string) *Builder[Req, Res] {
	b.meta.RateLimit = fmt.Sprintf("%.1frps (keyed)", rps)
	return b.rateLimitWithLimiter(newMemoryRateLimiter(rps, burst), keyFn)
}

func (b *Builder[Req, Res]) RateLimitDistributed(limiter RateLimiter, keyFn func(context.Context) string) *Builder[Req, Res] {
	b.meta.RateLimit = "distributed"
	return b.rateLimitWithLimiter(limiter, keyFn)
}

func (b *Builder[Req, Res]) rateLimitWithLimiter(limiter RateLimiter, keyFn func(context.Context) string) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if ctx != nil && ctx.Err() != nil {
				var zero Res
				return zero, ctx.Err()
			}
			if limiter == nil {
				return next(ctx, req)
			}
			key := ""
			if keyFn != nil {
				key = keyFn(ctx)
			}
			allowed, err := limiter.Allow(ctx, key)
			if err != nil {
				var zero Res
				if ctx != nil && ctx.Err() != nil {
					return zero, ctx.Err()
				}
				return zero, xerr.Internal("rate limiter error", err)
			}
			if !allowed {
				var zero Res
				return zero, xerr.TooManyRequests("rate limit exceeded")
			}
			return next(ctx, req)
		}
	})
}

type keyBucket struct {
	key      string
	limiter  *rate.Limiter
	lastSeen time.Time
	prev     *keyBucket
	next     *keyBucket
}

type memoryRateLimiter struct {
	mu          sync.Mutex
	m           map[string]*keyBucket
	head        *keyBucket // newest (MRU)
	tail        *keyBucket // oldest (LRU)
	rps         rate.Limit
	burst       int
	ttl         time.Duration
	maxCapacity int
}

func newMemoryRateLimiter(rps float64, burst int) *memoryRateLimiter {
	return newMemoryRateLimiterWithConfig(rps, burst, DefaultRateLimiterTTL, DefaultRateLimiterCapacity)
}

func newMemoryRateLimiterWithConfig(rps float64, burst int, ttl time.Duration, maxCapacity int) *memoryRateLimiter {
	if burst < 1 {
		burst = 1
	}
	if ttl <= 0 {
		ttl = DefaultRateLimiterTTL
	}
	if maxCapacity <= 0 {
		maxCapacity = DefaultRateLimiterCapacity
	}
	return &memoryRateLimiter{
		m:           make(map[string]*keyBucket),
		rps:         rate.Limit(rps),
		burst:       burst,
		ttl:         ttl,
		maxCapacity: maxCapacity,
	}
}

func (l *memoryRateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	if ctx != nil && ctx.Err() != nil {
		return false, ctx.Err()
	}

	if key == "" {
		key = "global"
	}

	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Amortized TTL eviction from the tail.
	// Since the list is strictly ordered by lastSeen (oldest at tail),
	// if tail is not expired, NO bucket in the limiter is expired.
	for range maxEvictPerCall {
		if l.tail == nil || now.Sub(l.tail.lastSeen) <= l.ttl {
			break
		}
		l.remove(l.tail)
	}

	// 2. Existing key hit
	bucket, ok := l.m[key]
	if ok {
		if now.Sub(bucket.lastSeen) > l.ttl {
			bucket.limiter = rate.NewLimiter(l.rps, l.burst)
		}
		bucket.lastSeen = now
		l.moveToHead(bucket)
		return bucket.limiter.Allow(), nil
	}

	// 3. Max capacity check (evicts oldest LRU item to guarantee bounded memory)
	if len(l.m) >= l.maxCapacity && l.tail != nil {
		l.remove(l.tail)
	}

	// 4. Create new key bucket at head
	bucket = &keyBucket{
		key:      key,
		limiter:  rate.NewLimiter(l.rps, l.burst),
		lastSeen: now,
	}
	l.m[key] = bucket
	l.pushHead(bucket)

	return bucket.limiter.Allow(), nil
}

func (l *memoryRateLimiter) pushHead(b *keyBucket) {
	b.prev = nil
	b.next = l.head
	if l.head != nil {
		l.head.prev = b
	}
	l.head = b
	if l.tail == nil {
		l.tail = b
	}
}

func (l *memoryRateLimiter) remove(b *keyBucket) {
	if b.prev != nil {
		b.prev.next = b.next
	} else {
		l.head = b.next
	}

	if b.next != nil {
		b.next.prev = b.prev
	} else {
		l.tail = b.prev
	}

	b.prev = nil
	b.next = nil
	delete(l.m, b.key)
}

func (l *memoryRateLimiter) moveToHead(b *keyBucket) {
	if l.head == b {
		return
	}

	if b.prev != nil {
		b.prev.next = b.next
	}
	if b.next != nil {
		b.next.prev = b.prev
	} else {
		l.tail = b.prev
	}

	b.prev = nil
	b.next = l.head
	if l.head != nil {
		l.head.prev = b
	}
	l.head = b
}
