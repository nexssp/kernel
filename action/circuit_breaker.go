// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// AdaptiveConfig tunes circuit breaker thresholds and timeout bounds.
type AdaptiveConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	InitialTimeout   time.Duration
}

type circuitState uint8

type adaptiveState struct {
	mu          sync.RWMutex
	state       circuitState
	failures    int
	lastFailure time.Time
	timeout     time.Duration
	cfg         AdaptiveConfig
	name        string
}

func timeoutCtx(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= d {
		// Parent context already has a stricter or equal deadline — skip wrapper allocation.
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// Adaptive wraps an action with a dynamic timeout and a stateful circuit breaker.
func Adaptive[Req, Res any](name string, cfg AdaptiveConfig) Middleware[Req, Res] {
	cfg.SetDefaults()
	state := &adaptiveState{
		state:   stateClosed,
		timeout: cfg.InitialTimeout,
		cfg:     cfg,
		name:    name,
	}

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !state.AllowRequest() {
				var zero Res
				return zero, xerr.CircuitBreaker("adaptive: circuit open")
			}

			tCtx, cancel := timeoutCtx(ctx, state.CurrentTimeout())
			defer cancel()

			res, err := next(tCtx, req)

			if err != nil && ctx.Err() != nil {
				state.ReleaseProbe()
				return res, err
			}

			state.Observe(err)
			return res, err
		}
	}
}
