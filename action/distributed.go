package action

import (
	"context"
	"log/slog"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// ErrLocked is returned when an action is already locked by another instance.
var ErrLocked = xerr.Conflict("resource is currently locked by another instance")

// Mutex defines a standard distributed lock contract for critical sections.
// For systems requiring protection against stale writers during process pauses,
// prefer FencedMutex via ExclusiveFenced.
type Mutex interface {
	TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key string) error
}

// Exclusive wraps an action with a standard distributed lock.
// If the lock cannot be acquired, ErrLocked is returned immediately.
func (b *Builder[Req, Res]) Exclusive(m Mutex, ttl time.Duration, keyFn func(Req) string) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			var zero Res
			if m == nil {
				return zero, xerr.Internal("mutex is required")
			}

			key := keyFn(req)
			if key == "" {
				return next(ctx, req)
			}

			lockKey := b.meta.Name + ":lock:" + key

			acquired, err := m.TryLock(ctx, lockKey, ttl)
			if err != nil {
				return zero, xerr.Unavailable("failed to acquire distributed lock", err)
			}
			if !acquired {
				return zero, ErrLocked
			}

			defer func() {
				if err := m.Unlock(context.WithoutCancel(ctx), lockKey); err != nil {
					slog.Error("distributed_lock_release_failed", "lock", lockKey, "error", err)
				}
			}()

			return next(ctx, req)
		}
	})
}

// LeaderOnly restricts action execution to a single instance using a global leader key.
func (b *Builder[Req, Res]) LeaderOnly(m Mutex, ttl time.Duration) *Builder[Req, Res] {
	return b.Exclusive(m, ttl, func(_ Req) string {
		return "global_leader"
	})
}
