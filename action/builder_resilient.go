package action

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"

	"golang.org/x/time/rate"
)

type RateLimiter interface {
	Allow(ctx context.Context, key string) (bool, error)
}

type ResilienceConfig struct {
	MaxRetries    int
	Backoff       func(attempt int) time.Duration
	Timeout       time.Duration
	MaxConcurrent int32
	Adaptive      *AdaptiveConfig
}

func (b *Builder[Req, Res]) Resilient(cfg ResilienceConfig) *Builder[Req, Res] {
	if cfg.MaxRetries > 0 {
		backoff := cfg.Backoff
		if backoff == nil {
			backoff = ExponentialJitter(200*time.Millisecond, 5*time.Second)
		}
		b = b.Retry(cfg.MaxRetries, backoff)
	}

	if cfg.Timeout > 0 {
		b = b.Timeout(cfg.Timeout)
	}

	if cfg.MaxConcurrent > 0 {
		b = b.ConcurrencyLimit(cfg.MaxConcurrent)
	}

	if cfg.Adaptive != nil {
		b = b.Use(Adaptive[Req, Res](b.meta.Name, *cfg.Adaptive))
	}

	return b
}

func (b *Builder[Req, Res]) RateLimit(requestsPerSecond float64, burst int) *Builder[Req, Res] {
	b.meta.RateLimit = fmt.Sprintf("%.1frps (burst %d)", requestsPerSecond, burst)
	limiter := rate.NewLimiter(rate.Limit(requestsPerSecond), burst)

	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
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
			key := keyFn(ctx)
			allowed, err := limiter.Allow(ctx, key)
			if err != nil {
				var zero Res
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
	limiter  *rate.Limiter
	lastSeen time.Time
}

type memoryRateLimiter struct {
	mu        sync.Mutex
	m         map[string]*keyBucket
	rps       rate.Limit
	burst     int
	ttl       time.Duration
	lastSweep time.Time
}

func newMemoryRateLimiter(rps float64, burst int) *memoryRateLimiter {
	return &memoryRateLimiter{
		m:         make(map[string]*keyBucket),
		rps:       rate.Limit(rps),
		burst:     burst,
		ttl:       3 * time.Minute,
		lastSweep: time.Now(),
	}
}

func (l *memoryRateLimiter) Allow(_ context.Context, key string) (bool, error) {
	if key == "" {
		key = "global"
	}

	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	if now.Sub(l.lastSweep) > time.Minute {
		for k, b := range l.m {
			if now.Sub(b.lastSeen) > l.ttl {
				delete(l.m, k)
			}
		}
		l.lastSweep = now
	}

	bucket, ok := l.m[key]
	if ok {
		bucket.lastSeen = now
		return bucket.limiter.Allow(), nil
	}

	lim := rate.NewLimiter(l.rps, l.burst)
	l.m[key] = &keyBucket{
		limiter:  lim,
		lastSeen: now,
	}
	return lim.Allow(), nil
}
