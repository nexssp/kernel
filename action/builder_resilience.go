// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"log/slog"
	"time"

	"github.com/nexssp/kernel/xerr"
)

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

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

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
	if xerr.IsPermanent(err) {
		a.mu.Lock()
		if a.state == stateHalfOpen {
			a.state = stateClosed
			a.failures = 0
			a.timeout = a.cfg.InitialTimeout
		}
		a.mu.Unlock()
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

// ReleaseProbe returns the breaker to Open after a caller-canceled probe.
// Without this, one canceled probe leaves the breaker permanently
// half-open and rejects every future request.
func (a *adaptiveState) ReleaseProbe() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state == stateHalfOpen {
		a.state = stateOpen
		a.lastFailure = time.Now()
	}
}

type ResilienceConfig struct {
	MaxRetries    int
	Backoff       func(attempt int) time.Duration
	Predicate     RetryPredicate
	Timeout       time.Duration
	MaxConcurrent int32
	Adaptive      *AdaptiveConfig
}

func (b *Builder[Req, Res]) Resilient(cfg ResilienceConfig) *Builder[Req, Res] {
	if cfg.MaxRetries > 0 {
		backoff := cfg.Backoff
		if backoff == nil {
			backoff = ExponentialJitter(200*time.Millisecond, 5*time.Second)
		}

		if cfg.Predicate != nil {
			b = b.RetryIf(cfg.MaxRetries, backoff, cfg.Predicate)
		} else {
			b = b.Retry(cfg.MaxRetries, backoff)
		}
	}

	if cfg.Timeout > 0 {
		b = b.Timeout(cfg.Timeout)
	}

	if cfg.MaxConcurrent > 0 {
		b = b.ConcurrencyLimit(cfg.MaxConcurrent)
	}

	if cfg.Adaptive != nil {
		b = b.Use(Adaptive[Req, Res](b.meta.Name, *cfg.Adaptive))
	}

	return b
}

// Retry retries only transient errors detected by xerr.IsTransient.
func (b *Builder[Req, Res]) Retry(
	maxRetry int,
	backoff func(attempt int) time.Duration,
) *Builder[Req, Res] {
	b.meta.RetryMax = maxRetry
	return b.UseWithDispatcher(RetryMiddleware[Req, Res](maxRetry, backoff))
}

// RetryIf retries when the supplied predicate returns true.
func (b *Builder[Req, Res]) RetryIf(
	maxRetry int,
	backoff func(attempt int) time.Duration,
	predicate RetryPredicate,
) *Builder[Req, Res] {
	b.meta.RetryMax = maxRetry
	return b.UseWithDispatcher(
		RetryWithPredicateMiddleware[Req, Res](maxRetry, backoff, predicate),
	)
}

// RetryAll retries on ANY non-nil error.
// Use only for idempotent jobs, scripts, and safe batch operations.
func (b *Builder[Req, Res]) RetryAll(
	maxRetry int,
	backoff func(attempt int) time.Duration,
) *Builder[Req, Res] {
	b.meta.RetryMax = maxRetry
	return b.UseWithDispatcher(
		RetryWithPredicateMiddleware[Req, Res](maxRetry, backoff, AlwaysRetryPredicate),
	)
}

// SmartResilience automatically applies intelligent backoff and circuit breaking
// based entirely on the xerr.Kind of the returned error.
// It delegates to the universal RetryIf middleware to ensure 100% hook/telemetry fidelity.
func SmartResilience[Req, Res any](name string) Middleware[Req, Res] {
	cb := Adaptive[Req, Res](name, AdaptiveConfig{
		FailureThreshold: 5,
		ResetTimeout:     10 * time.Second,
		InitialTimeout:   3 * time.Second,
	})

	backoff := ExponentialJitter(100*time.Millisecond, 2*time.Second)

	// Smart predicate: retry anything that is NOT a permanent 4xx error
	predicate := func(err error) bool {
		return err != nil && !xerr.IsPermanent(err)
	}

	retry := RetryWithPredicateMiddleware[Req, Res](3, backoff, predicate)

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		// Kolejność ma znaczenie: Retry wewnątrz Circuit Breakera
		// cb(retry(next)) -> błędy z retry uderzają w CB.
		return cb(retry(next, nil))
	}
}

func (b *Builder[Req, Res]) InferredResilient() *Builder[Req, Res] {
	return b.Use(SmartResilience[Req, Res](b.meta.Name))
}
