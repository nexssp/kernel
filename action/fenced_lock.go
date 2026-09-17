package action

import (
	"context"
	"errors"
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

			lockKey, err := validateFencedLock(m, ttl, b.meta.Name, keyFn(req))
			if err != nil {
				return zero, err
			}

			lease, acquired, err := m.Acquire(ctx, lockKey, ttl)
			if err != nil {
				return zero, xerr.Unavailable("failed to acquire fenced distributed lock", err)
			}
			if !acquired {
				return zero, ErrLocked
			}

			execCtx, cancel := context.WithCancel(ctx)
			execCtx = context.WithValue(execCtx, leaseCtxKey{}, lease)

			done := make(chan struct{})
			lost := make(chan error, 1)
			var renewWG sync.WaitGroup
			renewWG.Go(func() {
				runLeaseRenewer(execCtx, m, lease, ttl, done, lost, cancel, lockKey, b.meta.Name)
			})

			var releaseOnce sync.Once
			cleanup := func() {
				releaseOnce.Do(func() {
					stopRenewer(ctx, done, &renewWG, m, lease, lockKey, b.meta.Name)
				})
			}

			defer func() {
				cancel()
				cleanup()
			}()

			res, execErr := next(execCtx, req)
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

func validateFencedLock(m FencedMutex, ttl time.Duration, actionName, key string) (string, error) {
	if m == nil {
		return "", xerr.Internal("fenced mutex is required")
	}
	if ttl < minTTL {
		return "", xerr.BadRequest(fmt.Sprintf("fenced lock TTL must be at least %v", minTTL))
	}
	if key == "" {
		return "", xerr.BadRequest("fenced lock key cannot be empty")
	}
	return actionName + ":lock:" + key, nil
}

func runLeaseRenewer(
	execCtx context.Context,
	m FencedMutex,
	lease LockLease,
	ttl time.Duration,
	done chan struct{},
	lost chan<- error,
	cancel context.CancelFunc,
	lockKey, actionName string,
) {
	reportLoss := func(cause error) {
		select {
		case lost <- cause:
		default:
			slog.Error("fenced_lock_additional_error",
				"action", actionName, "lock", lockKey, "error", cause)
		}
		cancel()
	}

	defer func() {
		if r := recover(); r != nil {
			reportLoss(fmt.Errorf("fenced lock renew panic: %v", r))
		}
	}()

	interval := ttl / 3
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
			if isShuttingDown(done) {
				return
			}
			if execCtx.Err() == nil {
				reportLoss(fmt.Errorf("renew fenced lock: %w", renewErr))
			}
			return
		}
		if !renewed {
			if isShuttingDown(done) || execCtx.Err() != nil {
				return
			}
			reportLoss(errors.New("fenced lock lease lost"))
			return
		}
	}
}

func isShuttingDown(done chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func stopRenewer(
	parentCtx context.Context,
	done chan struct{},
	wg *sync.WaitGroup,
	m FencedMutex,
	lease LockLease,
	lockKey, actionName string,
) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("fenced_lock_release_panic",
				"action", actionName, "lock", lockKey, "panic", r)
		}
	}()
	close(done)
	wg.Wait()

	releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(parentCtx), 5*time.Second)
	defer releaseCancel()

	if _, releaseErr := m.Release(releaseCtx, lease); releaseErr != nil {
		slog.Error("fenced_lock_release_failed",
			"action", actionName, "lock", lockKey,
			"owner", lease.Owner, "error", releaseErr)
	}
}

// LeaderOnlyFenced is the safe singleton-action form. It provides a renewable
// ownership lease, not merely a best-effort process-local convention.
func (b *Builder[Req, Res]) LeaderOnlyFenced(m FencedMutex, ttl time.Duration) *Builder[Req, Res] {
	return b.ExclusiveFenced(m, ttl, func(_ Req) string { return "global_leader" })
}
