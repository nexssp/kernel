package action

import (
	"context"
	"log/slog"
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

func (c *AdaptiveConfig) SetDefaults() {
	if c.FailureThreshold == 0 {
		c.FailureThreshold = 5
	}
	if c.ResetTimeout == 0 {
		c.ResetTimeout = 30 * time.Second
	}
	if c.InitialTimeout == 0 {
		c.InitialTimeout = 5 * time.Second
	}
}

type circuitState uint8

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

type adaptiveState struct {
	mu          sync.RWMutex
	state       circuitState
	failures    int
	lastFailure time.Time
	timeout     time.Duration
	cfg         AdaptiveConfig
	name        string
}

func (a *adaptiveState) CurrentTimeout() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.timeout
}

// AllowRequest implements the 3-state transition logic.
// Exactly one probe request is allowed through during the half-open window.
func (a *adaptiveState) AllowRequest() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()

	switch a.state {
	case stateClosed:
		return true

	case stateOpen:
		if now.Sub(a.lastFailure) > a.cfg.ResetTimeout {
			slog.Info("CircuitBreaker: Half-Open transition", "action", a.name)
			a.state = stateHalfOpen
			return true
		}
		return false

	case stateHalfOpen:
		// Another probe is currently active; reject concurrent callers
		return false
	}

	return false
}

func (a *adaptiveState) Observe(err error) {
	if err == nil {
		a.mu.Lock()
		if a.state != stateClosed {
			slog.Info("CircuitBreaker: Closed transition", "action", a.name)
		}
		a.state = stateClosed
		a.failures = 0
		a.timeout = a.cfg.InitialTimeout
		a.mu.Unlock()
		return
	}

	// Never trip circuit breaker on permanent client errors (4xx)
	switch xerr.KindFrom(err) {
	case xerr.KindBadRequest, xerr.KindUnauthorized, xerr.KindForbidden, xerr.KindNotFound, xerr.KindValidation:
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.failures++
	a.lastFailure = time.Now()

	if a.state == stateHalfOpen || a.failures >= a.cfg.FailureThreshold {
		a.state = stateOpen
		a.timeout = time.Duration(float64(a.cfg.InitialTimeout) * (1 + float64(a.failures)/10))
		slog.Warn("CircuitBreaker: OPEN", "action", a.name, "failures", a.failures)
	}
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

			tCtx, cancel := context.WithTimeout(ctx, state.CurrentTimeout())
			defer cancel()

			res, err := next(tCtx, req)

			if err != nil && ctx.Err() != nil {
				return res, err
			}

			state.Observe(err)
			return res, err
		}
	}
}
