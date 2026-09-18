// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// Admission controls whether an execution may enter a protected section.
// Implementations may be local or distributed. Acquire must return an error
// without retaining the request when admission is denied.
type Admission interface {
	Acquire(context.Context) error
	Release()
}

// AdmissionMiddleware applies an external admission policy around execution.
// It keeps the policy outside Builder while allowing typed middleware
// composition and automatic request/response inference.
func AdmissionMiddleware[Req, Res any](admission Admission) DispatcherMiddleware[Req, Res] {
	return func(next Fn[Req, Res], _ HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			if err := admission.Acquire(ctx); err != nil {
				return res, err
			}
			defer admission.Release()
			return next(ctx, req)
		}
	}
}

var ErrConcurrencyLimit = errors.New("concurrency limit exceeded")

func ConcurrencyLimitMiddleware[Req, Res any](limit int32) Middleware[Req, Res] {
	var count atomic.Int32

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if count.Add(1) > limit {
				count.Add(-1)
				var zero Res
				return zero, ErrConcurrencyLimit
			}
			defer count.Add(-1)

			return next(ctx, req)
		}
	}
}

type Priority uint8

const (
	PriorityCritical Priority = 0 // Payments, Logins
	PriorityNormal   Priority = 1 // Standard CRUD
	PriorityLow      Priority = 2 // Background syncs, Exports
)

// SystemStats is implemented lock-free by nexss/monitor.
type SystemStats interface {
	CPUPercent() float64
	Goroutines() int
}

type LoadShedConfig struct {
	MaxCPU        float64
	MaxGoroutines int
}

// AdaptiveLoadShedding drops traffic instantly if the server is physically choking.
func AdaptiveLoadShedding[Req, Res any](stats SystemStats, cfg LoadShedConfig, p Priority) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if stats != nil {
				cpu := stats.CPUPercent()
				goroutines := stats.Goroutines()

				// Progressive load shedding based on Priority
				var overloaded bool
				switch p {
				case PriorityLow:
					overloaded = (cfg.MaxCPU > 0 && cpu > cfg.MaxCPU*0.7) || (cfg.MaxGoroutines > 0 && goroutines > int(float64(cfg.MaxGoroutines)*0.7))
				case PriorityNormal:
					overloaded = (cfg.MaxCPU > 0 && cpu > cfg.MaxCPU*0.85) || (cfg.MaxGoroutines > 0 && goroutines > int(float64(cfg.MaxGoroutines)*0.85))
				case PriorityCritical:
					overloaded = (cfg.MaxCPU > 0 && cpu > cfg.MaxCPU) || (cfg.MaxGoroutines > 0 && goroutines > cfg.MaxGoroutines)
				}

				if overloaded {
					var zero Res
					return zero, xerr.Unavailable("server actively shedding load to preserve stability")
				}
			}
			return next(ctx, req)
		}
	}
}

// TimeoutMiddleware uses standard Middleware.
func TimeoutMiddleware[Req, Res any](d time.Duration) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(ctx, req)
		}
	}
}
