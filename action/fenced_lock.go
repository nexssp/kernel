package action

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// LockLease proves ownership of a distributed lock. Fence is monotonically
// increasing for a key and must be carried to any downstream system that can
// reject stale writers.
type LockLease struct {
	Key   string
	Owner string
	Fence int64
}

// FencedMutex is the production-safe distributed coordination contract. A
// lease belongs to exactly one owner, can be renewed only by that owner, and
// can be released only by that owner.
type FencedMutex interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (lease LockLease, acquired bool, err error)
	Renew(ctx context.Context, lease LockLease, ttl time.Duration) (renewed bool, err error)
	Release(ctx context.Context, lease LockLease) (released bool, err error)
}

// ExclusiveFenced runs an action under an ownership-checked lease. The lease
// is renewed while the action is running; loss of the lease cancels the action
// context and returns an unavailable error rather than claiming success.
func (b *Builder[Req, Res]) ExclusiveFenced(m FencedMutex, ttl time.Duration, keyFn func(Req) string) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			var zero Res
			if m == nil {
				return zero, xerr.Internal("fenced mutex is required")
			}
			if ttl <= 0 {
				return zero, xerr.BadRequest("fenced lock TTL must be positive")
			}

			key := keyFn(req)
			if key == "" {
				return next(ctx, req)
			}
			lockKey := b.meta.Name + ":lock:" + key
			lease, acquired, err := m.Acquire(ctx, lockKey, ttl)
			if err != nil {
				return zero, xerr.Unavailable("failed to acquire fenced distributed lock", err)
			}
			if !acquired {
				return zero, ErrLocked
			}

			execCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan struct{})
			lost := make(chan error, 1)
			var renewWG sync.WaitGroup
			renewWG.Add(1)
			go func() {
				defer renewWG.Done()
				interval := ttl / 3
				if interval < 100*time.Millisecond {
					interval = 100 * time.Millisecond
				}
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-execCtx.Done():
						return
					case <-ticker.C:
						renewed, renewErr := m.Renew(context.WithoutCancel(ctx), lease, ttl)
						if renewErr != nil {
							select {
							case lost <- fmt.Errorf("renew fenced lock: %w", renewErr):
							default:
							}
							cancel()
							return
						}
						if !renewed {
							select {
							case lost <- fmt.Errorf("fenced lock lease lost"):
							default:
							}
							cancel()
							return
						}
					}
				}
			}()

			res, execErr := next(execCtx, req)
			close(done)
			renewWG.Wait()

			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_, releaseErr := m.Release(releaseCtx, lease)
			releaseCancel()
			if releaseErr != nil {
				return zero, xerr.Unavailable("failed to release fenced distributed lock", releaseErr)
			}
			select {
			case lostErr := <-lost:
				return zero, xerr.Unavailable("fenced distributed lock lease lost", lostErr)
			default:
			}
			return res, execErr
		}
	})
}

// LeaderOnlyFenced is the safe singleton-action form. It provides a renewable
// ownership lease, not merely a best-effort process-local convention.
func (b *Builder[Req, Res]) LeaderOnlyFenced(m FencedMutex, ttl time.Duration) *Builder[Req, Res] {
	return b.ExclusiveFenced(m, ttl, func(_ Req) string { return "global_leader" })
}
