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

// DrainGuard is the typed middleware; use it for any Req/Res.
func DrainGuard[Req, Res any](d *DrainSupervisor) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			// Increment before checking: Drain either sees us in flight or we see draining.
			d.inFlight.Add(1)
			defer d.exit()

			if d.draining.Load() {
				var zero Res
				return zero, xerr.Shutdown("system is shutting down; request rejected")
			}
			return next(ctx, req)
		}
	}
}

// Middleware is DrainGuard for Builder[any, any].
func (d *DrainSupervisor) Middleware() Middleware[any, any] {
	return DrainGuard[any, any](d)
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
