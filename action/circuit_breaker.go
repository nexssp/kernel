// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
)

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

type circuitState uint8

// CircuitBreakerConfig controls the failure threshold and recovery window of a
// standalone circuit breaker. Zero fields use the defaults returned by
// Normalized; negative values are invalid.
type CircuitBreakerConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
}

// Normalized returns cfg with the standalone circuit-breaker defaults filled in.
func (cfg CircuitBreakerConfig) Normalized() CircuitBreakerConfig {
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.ResetTimeout == 0 {
		cfg.ResetTimeout = 30 * time.Second
	}
	return cfg
}

// Validate reports invalid circuit-breaker settings after applying defaults.
func (cfg CircuitBreakerConfig) Validate() error {
	cfg = cfg.Normalized()
	if cfg.FailureThreshold < 1 {
		return xerr.Validation("circuit breaker: failure_threshold must be positive")
	}
	if cfg.ResetTimeout <= 0 {
		return xerr.Validation("circuit breaker: reset_timeout must be positive")
	}
	return nil
}

// CircuitBreakerState owns the mutable state for one logical breaker key.
// Create states with NewCircuitBreakerState or obtain them from a
// CircuitBreakerRegistry; a zero-value state is deliberately invalid.
type CircuitBreakerState struct {
	mu          sync.Mutex
	state       circuitState
	failures    int
	openedAt    time.Time
	probeID     uint64
	cfg         CircuitBreakerConfig
	key         string
	initialized bool
}

// NewCircuitBreakerState creates an independent state machine with validated
// settings. Use a registry when multiple actions should share one breaker.
func NewCircuitBreakerState(key string, cfg CircuitBreakerConfig) (*CircuitBreakerState, error) {
	if key != trimSpace(key) {
		return nil, xerr.Validation("circuit breaker: key must not have surrounding whitespace")
	}
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &CircuitBreakerState{cfg: cfg, key: key, state: stateClosed, initialized: true}, nil
}

// CircuitBreakerRegistry shares states by key within one host-owned registry.
// Its zero value is ready for use. Separate registries never share state.
type CircuitBreakerRegistry struct {
	mu     sync.Mutex
	states map[string]*CircuitBreakerState
}

// NewCircuitBreakerRegistry creates an empty, instance-scoped breaker registry.
func NewCircuitBreakerRegistry() *CircuitBreakerRegistry {
	return &CircuitBreakerRegistry{states: make(map[string]*CircuitBreakerState)}
}

// GetOrCreate returns the state registered for key, creating it if necessary.
// Reusing a key with a different normalized configuration is an error.
func (r *CircuitBreakerRegistry) GetOrCreate(key string, cfg CircuitBreakerConfig) (*CircuitBreakerState, error) {
	if r == nil {
		return nil, xerr.Validation("circuit breaker: registry is nil")
	}
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if key == "" || key != trimSpace(key) {
		return nil, xerr.Validation("circuit breaker: registry key must be non-empty and have no surrounding whitespace")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = make(map[string]*CircuitBreakerState)
	}
	if existing := r.states[key]; existing != nil {
		if existing.cfg != cfg {
			return nil, xerr.Conflict(fmt.Sprintf("circuit breaker %q: configuration differs from the existing registry entry", key))
		}
		return existing, nil
	}
	state, err := NewCircuitBreakerState(key, cfg)
	if err != nil {
		return nil, err
	}
	r.states[key] = state
	return state, nil
}

type breakerPermit struct {
	probeID uint64
	probe   bool
}

type breakerResult struct {
	opened   bool
	closed   bool
	failures int
}

func (s *CircuitBreakerState) valid() bool {
	return s != nil && s.initialized && s.cfg.FailureThreshold > 0 && s.cfg.ResetTimeout > 0
}

func (s *CircuitBreakerState) allow() (breakerPermit, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case stateClosed:
		return breakerPermit{}, true
	case stateOpen:
		if time.Since(s.openedAt) < s.cfg.ResetTimeout {
			return breakerPermit{}, false
		}
		s.state = stateHalfOpen
		s.probeID++
		return breakerPermit{probeID: s.probeID, probe: true}, true
	case stateHalfOpen:
		return breakerPermit{}, false
	default:
		return breakerPermit{}, false
	}
}

// complete records one admitted call. A permit identifies the sole half-open
// probe so late completions from older closed-state calls cannot close a breaker
// that has since opened.
func (s *CircuitBreakerState) complete(permit breakerPermit, err error, canceled bool) breakerResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if canceled {
		if permit.probe && s.state == stateHalfOpen && permit.probeID == s.probeID {
			s.state = stateOpen
			s.openedAt = time.Now()
		}
		return breakerResult{failures: s.failures}
	}

	if permit.probe {
		if s.state != stateHalfOpen || permit.probeID != s.probeID {
			return breakerResult{failures: s.failures}
		}
		// Success and permanent errors preserve the existing close behavior.
		if err == nil || xerr.IsPermanent(err) {
			s.state = stateClosed
			s.failures = 0
			return breakerResult{closed: true}
		}
		// An unclassified probe is inconclusive: keep the breaker open for
		// another recovery window, but do not count it as an outage failure.
		if !xerr.IsTransient(err) {
			s.state = stateOpen
			s.openedAt = time.Now()
			return breakerResult{failures: s.failures}
		}
		s.failures++
		s.state = stateOpen
		s.openedAt = time.Now()
		return breakerResult{opened: true, failures: s.failures}
	}

	if s.state != stateClosed {
		return breakerResult{failures: s.failures}
	}
	if err == nil {
		s.failures = 0
		return breakerResult{closed: true}
	}
	// Do not treat unclassified errors (for example, Internal or Canceled) as
	// transient. The breaker tracks outages only when xerr says they are retryable.
	if !xerr.IsTransient(err) {
		return breakerResult{failures: s.failures}
	}
	s.failures++
	if s.failures >= s.cfg.FailureThreshold {
		s.state = stateOpen
		s.openedAt = time.Now()
		return breakerResult{opened: true, failures: s.failures}
	}
	return breakerResult{failures: s.failures}
}

func trimSpace(s string) string {
	return strings.TrimSpace(s)
}

// CircuitBreaker wraps next with a standalone circuit breaker. The supplied
// state may be shared by actions via a CircuitBreakerRegistry. A nil or
// zero-value state fails closed with a validation error instead of panicking.
func CircuitBreaker[Req, Res any](state *CircuitBreakerState) Middleware[Req, Res] {
	if !state.valid() {
		return func(Fn[Req, Res]) Fn[Req, Res] {
			return func(context.Context, Req) (Res, error) {
				var zero Res
				return zero, xerr.Validation("circuit breaker: state is uninitialized")
			}
		}
	}
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			permit, allowed := state.allow()
			if !allowed {
				return res, xerr.CircuitBreaker(fmt.Sprintf("circuit breaker %q is open", state.key))
			}
			defer func() {
				if recovered := recover(); recovered != nil {
					state.complete(permit, xerr.PanicRecovery(recovered), ctx.Err() != nil)
					panic(recovered)
				}
			}()
			res, err = next(ctx, req)
			state.complete(permit, err, err != nil && ctx.Err() != nil)
			return res, err
		}
	}
}

// AdaptiveConfig tunes both the circuit breaker and its independent dynamic
// timeout. Adaptive remains distinct from the standalone CircuitBreaker.
type AdaptiveConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	InitialTimeout   time.Duration
}

// Normalized returns cfg with the existing Adaptive defaults filled in.
func (cfg *AdaptiveConfig) Normalized() AdaptiveConfig {
	normalized := AdaptiveConfig{}
	if cfg != nil {
		normalized = *cfg
	}
	breaker := (CircuitBreakerConfig{FailureThreshold: normalized.FailureThreshold, ResetTimeout: normalized.ResetTimeout}).Normalized()
	normalized.FailureThreshold = breaker.FailureThreshold
	normalized.ResetTimeout = breaker.ResetTimeout
	if normalized.InitialTimeout == 0 {
		normalized.InitialTimeout = 5 * time.Second
	}
	return normalized
}

// Validate reports invalid adaptive breaker or timeout settings after defaults.
func (cfg *AdaptiveConfig) Validate() error {
	normalized := cfg.Normalized()
	if err := (CircuitBreakerConfig{FailureThreshold: normalized.FailureThreshold, ResetTimeout: normalized.ResetTimeout}).Validate(); err != nil {
		return err
	}
	if normalized.InitialTimeout <= 0 {
		return xerr.Validation("adaptive: initial_timeout must be positive")
	}
	return nil
}

type adaptiveState struct {
	breaker *CircuitBreakerState
	cfg     AdaptiveConfig
	mu      sync.RWMutex
	timeout time.Duration
}

func (s *adaptiveState) currentTimeout() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.timeout
}

func (s *adaptiveState) complete(permit breakerPermit, err error, canceled bool) {
	result := s.breaker.complete(permit, err, canceled)
	if canceled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil || result.closed:
		s.timeout = s.cfg.InitialTimeout
	case result.opened:
		multiplier := 1 + float64(result.failures)/10
		s.timeout = time.Duration(float64(s.cfg.InitialTimeout) * multiplier)
	}
}

func timeoutCtx(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= d {
		// Parent context already has a stricter or equal deadline — skip wrapper allocation.
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// Adaptive wraps an action with a dynamic timeout and a stateful circuit breaker.
// Invalid settings produce a validation error when the middleware is invoked.
func Adaptive[Req, Res any](name string, cfg AdaptiveConfig) Middleware[Req, Res] {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return func(Fn[Req, Res]) Fn[Req, Res] {
			return func(context.Context, Req) (Res, error) {
				var zero Res
				return zero, xerr.Validation("adaptive configuration: "+err.Error(), err)
			}
		}
	}
	breaker, err := NewCircuitBreakerState(name, CircuitBreakerConfig{
		FailureThreshold: cfg.FailureThreshold,
		ResetTimeout:     cfg.ResetTimeout,
	})
	if err != nil {
		return func(Fn[Req, Res]) Fn[Req, Res] {
			return func(context.Context, Req) (Res, error) {
				var zero Res
				return zero, xerr.Validation("adaptive configuration: "+err.Error(), err)
			}
		}
	}
	state := &adaptiveState{breaker: breaker, cfg: cfg, timeout: cfg.InitialTimeout}
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			permit, allowed := state.breaker.allow()
			if !allowed {
				return res, xerr.CircuitBreaker("adaptive: circuit open")
			}
			timedCtx, cancel := timeoutCtx(ctx, state.currentTimeout())
			defer cancel()
			defer func() {
				if recovered := recover(); recovered != nil {
					state.complete(permit, xerr.PanicRecovery(recovered), ctx.Err() != nil)
					panic(recovered)
				}
			}()
			res, err = next(timedCtx, req)
			state.complete(permit, err, err != nil && ctx.Err() != nil)
			return res, err
		}
	}
}
