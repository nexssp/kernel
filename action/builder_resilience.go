// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"errors"
	"fmt"
	"time"

	"github.com/nexssp/kernel/xerr"
)

func (c *AdaptiveConfig) SetDefaults() {
	if c != nil {
		*c = c.Normalized()
	}
}

type ResilienceConfig struct {
	MaxRetries     int
	Backoff        func(attempt int) time.Duration
	Predicate      RetryPredicate
	Timeout        time.Duration
	MaxConcurrent  int32
	CircuitBreaker *CircuitBreakerState
	Adaptive       *AdaptiveConfig
}

// ApplyResilience validates cfg before mutating the builder, then installs
// policies in one deterministic order: concurrency admission, breaker (or
// Adaptive), retry loop, per-attempt timeout, then any existing inner
// middleware and the handler. Call it at most once per builder.
func (b *Builder[Req, Res]) ApplyResilience(cfg ResilienceConfig) error {
	if b == nil {
		return errors.New("resilience: builder is nil")
	}
	if b.resilienceApplied {
		return errors.New("resilience: policies have already been applied to this builder")
	}
	if cfg.MaxRetries < 0 {
		return errors.New("resilience: max retries must not be negative")
	}
	if cfg.Timeout < 0 {
		return errors.New("resilience: timeout must not be negative")
	}
	if cfg.MaxConcurrent < 0 {
		return errors.New("resilience: max concurrent must not be negative")
	}
	if cfg.CircuitBreaker != nil && cfg.Adaptive != nil {
		return errors.New("resilience: standalone circuit breaker and adaptive breaker are mutually exclusive")
	}
	if cfg.CircuitBreaker != nil && !cfg.CircuitBreaker.valid() {
		return errors.New("resilience: circuit breaker state is uninitialized")
	}
	if cfg.Adaptive != nil {
		normalized := cfg.Adaptive.Normalized()
		if err := normalized.Validate(); err != nil {
			return fmt.Errorf("resilience: %w", err)
		}
		cfg.Adaptive = &normalized
	}

	if cfg.MaxRetries > 0 {
		backoff := cfg.Backoff
		if backoff == nil {
			backoff = ExponentialJitter(200*time.Millisecond, 5*time.Second)
		}

		if cfg.Predicate != nil {
			b.RetryIf(cfg.MaxRetries, backoff, cfg.Predicate)
		} else {
			b.Retry(cfg.MaxRetries, backoff)
		}
	}

	if cfg.Timeout > 0 {
		b.Timeout(cfg.Timeout)
	}

	if cfg.CircuitBreaker != nil {
		b.UseFirst(CircuitBreaker[Req, Res](cfg.CircuitBreaker))
	}
	if cfg.Adaptive != nil {
		b.UseFirst(Adaptive[Req, Res](b.meta.Name, *cfg.Adaptive))
	}

	if cfg.MaxConcurrent > 0 {
		b.meta.ConcurrencyLimit = cfg.MaxConcurrent
		b.UseFirst(ConcurrencyLimitMiddleware[Req, Res](cfg.MaxConcurrent))
	}
	b.resilienceApplied = true
	return nil
}

// Resilient preserves the fluent API. Invalid settings are stored and returned
// by the built action's Do method; use ApplyResilience for immediate errors.
func (b *Builder[Req, Res]) Resilient(cfg ResilienceConfig) *Builder[Req, Res] {
	if err := b.ApplyResilience(cfg); err != nil && b.validationErr == nil {
		b.validationErr = err
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
func SmartResilience[Req, Res any](name string) DispatcherMiddleware[Req, Res] {
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

	return func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res] {
		// Order matters: Retry inside Circuit Breaker.
		// cb(retry(next, hooks)) -> retry failures hit the CB,
		// and the retry middleware gets the real dispatcher so OnRetry,
		// observe.Hook(sink), Instrument, etc. all fire.
		return cb(retry(next, hooks))
	}
}

func (b *Builder[Req, Res]) InferredResilient() *Builder[Req, Res] {
	return b.UseWithDispatcher(SmartResilience[Req, Res](b.meta.Name))
}
