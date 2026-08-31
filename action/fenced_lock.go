package action

import (
	"context"
	"fmt"
	"log/slog"
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

type leaseCtxKey struct{}

// LeaseFromContext retrieves the active LockLease from the execution context.
func LeaseFromContext(ctx context.Context) (LockLease, bool) {
	lease, ok := ctx.Value(leaseCtxKey{}).(LockLease)
	return lease, ok
}

const (
	minTTL = 300 * time.Millisecond
)

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
			if ttl < minTTL {
				return zero, xerr.BadRequest(fmt.Sprintf("fenced lock TTL must be at least %v", minTTL))
			}

			key := keyFn(req)
			if key == "" {
				return zero, xerr.BadRequest("fenced lock key cannot be empty")
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

			// Inject the lease so downstream handlers can verify the fence token.
			execCtx = context.WithValue(execCtx, leaseCtxKey{}, lease)

			done := make(chan struct{})
			lost := make(chan error, 1)
			var renewWG sync.WaitGroup
			renewWG.Add(1)

			reportLoss := func(cause error) {
				select {
				case lost <- cause:
				default:
					slog.Error("fenced_lock_additional_error",
						"action", b.meta.Name,
						"lock", lockKey,
						"error", cause,
					)
				}
				cancel()
			}

			interval := ttl / 3

			go func() {
				defer renewWG.Done()
				defer func() {
					if r := recover(); r != nil {
						reportLoss(fmt.Errorf("fenced lock renew panic: %v", r))
					}
				}()

				ticker := time.NewTicker(interval)
				defer ticker.Stop()

				for {
					select {
					case <-done:
						return
					case <-execCtx.Done():
						return
					case <-ticker.C:
					}

					renewCtx, renewCancel := context.WithTimeout(execCtx, interval)
					renewed, renewErr := m.Renew(renewCtx, lease, ttl)
					renewCancel()

					if renewErr != nil {
						// If we're shutting down normally, this is not a lease loss.
						select {
						case <-done:
							return
						default:
						}

						if execCtx.Err() == nil {
							reportLoss(fmt.Errorf("renew fenced lock: %w", renewErr))
						}
						return
					}

					if !renewed {
						select {
						case <-done:
							return
						default:
						}
						if execCtx.Err() != nil {
							return
						}
						reportLoss(fmt.Errorf("fenced lock lease lost"))
						return
					}
				}
			}()

			var releaseOnce sync.Once
			cleanup := func() {
				releaseOnce.Do(func() {
					defer func() {
						if r := recover(); r != nil {
							slog.Error("fenced_lock_release_panic",
								"action", b.meta.Name,
								"lock", lockKey,
								"panic", r,
							)
						}
					}()
					close(done)
					renewWG.Wait()

					releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					defer releaseCancel()

					if _, releaseErr := m.Release(releaseCtx, lease); releaseErr != nil {
						slog.Error("fenced_lock_release_failed",
							"action", b.meta.Name,
							"lock", lockKey,
							"owner", lease.Owner,
							"error", releaseErr,
						)
					}
				})
			}

			// Panic safety: cancel first to stop the renewer, then release.
			defer func() {
				cancel()
				cleanup()
			}()

			res, execErr := next(execCtx, req)

			// Normal path: stop the renewer before checking for lease loss.
			cancel()
			cleanup()

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
