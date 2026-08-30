package action

import (
	"context"

	"github.com/nexssp/kernel/xerr"
)

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
