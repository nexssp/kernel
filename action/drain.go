package action

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/nexssp/kernel/xerr"
)

// DrainSupervisor coordinates graceful shutdown across actions.
type DrainSupervisor struct {
	inFlight atomic.Int64
	draining atomic.Bool
	done     chan struct{}
	once     sync.Once
}

func NewDrainSupervisor() *DrainSupervisor {
	return &DrainSupervisor{done: make(chan struct{})}
}

// DrainGuard is typed middleware for guarding the wrapped execution. To reject
// before hooks and outer middleware, attach the supervisor with
// Builder.WithDrainSupervisor.
func DrainGuard[Req, Res any](d *DrainSupervisor) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !d.enter() {
				var zero Res
				return zero, xerr.Shutdown("system is shutting down; request rejected")
			}
			defer d.exit()
			return next(ctx, req)
		}
	}
}

// Middleware is DrainGuard for Builder[any, any].
func (d *DrainSupervisor) Middleware() Middleware[any, any] {
	return DrainGuard[any, any](d)
}

// WithDrainSupervisor rejects new Do calls before hooks and middleware once
// draining begins, and includes the whole action lifecycle in the drain wait.
func (b *Builder[Req, Res]) WithDrainSupervisor(d *DrainSupervisor) *Builder[Req, Res] {
	b.drain = d
	return b
}

func (d *DrainSupervisor) enter() bool {
	// Increment before checking: Drain either sees us in flight or we see draining.
	d.inFlight.Add(1)
	if d.draining.Load() {
		d.exit()
		return false
	}
	return true
}

func (d *DrainSupervisor) exit() {
	if d.inFlight.Add(-1) == 0 && d.draining.Load() {
		d.once.Do(func() { close(d.done) })
	}
}

// Drain rejects new requests and waits for in-flight ones or ctx expiry.
func (d *DrainSupervisor) Drain(ctx context.Context) error {
	d.draining.Store(true)

	if d.inFlight.Load() == 0 {
		d.once.Do(func() { close(d.done) })
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return nil
	}
}

func (d *DrainSupervisor) InFlight() int64 { return d.inFlight.Load() }

// Draining reports whether Drain has started.
func (d *DrainSupervisor) Draining() bool { return d.draining.Load() }
