// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexssp/kernel/ringbuf"
	"github.com/nexssp/kernel/xctx"
)

// Emits registers automatic event emission upon successful action execution.
// Payload mapping is evaluated lazily only when an active EventPublisher is in context.
func (b *Builder[Req, Res]) Emits(subject string, mapper func(res Res) any) *Builder[Req, Res] {
	return b.HookSuccess(func(ctx context.Context, _ Req, res Res, _ *Meta) {
		// Short-circuit: Exit without payload allocation if no publisher is configured
		pub, ok := xctx.EventPublisherFrom(ctx)
		if !ok || pub == nil {
			return
		}

		// Lazy evaluation: build DTO only when sending
		var payload any = res
		if mapper != nil {
			payload = mapper(res)
		}

		if err := pub.PublishEvent(ctx, subject, payload); err != nil {
			xctx.AddTrace(ctx, fmt.Sprintf("event_emit_failed:%s: %v", subject, err))
		}
	})
}

// WithHistory attaches a ring-buffer execution history and returns its handle.
// If allocation is rejected, the handle is nil and a built action returns the
// configuration error before running; use WithHistoryChecked to handle it now.
func (b *Builder[Req, Res]) WithHistory(capacity int) (*Builder[Req, Res], *History[Req, Res]) {
	builder, history, err := b.WithHistoryChecked(capacity)
	if err != nil {
		b.history = nil
		b.historyErr = fmt.Errorf("allocation: %w", err)
		return b, nil
	}
	return builder, history
}

// WithHistoryChecked attaches history using the default memory budget and
// returns an allocation error immediately.
func (b *Builder[Req, Res]) WithHistoryChecked(capacity int) (*Builder[Req, Res], *History[Req, Res], error) {
	return b.WithHistoryWithOptions(ringbuf.BufferOptions{
		Capacity:          capacity,
		MemoryBudgetBytes: ringbuf.DefaultMemoryBudgetBytes,
	})
}

// WithHistoryWithOptions attaches history using explicit buffer options and
// returns an allocation error immediately.
func (b *Builder[Req, Res]) WithHistoryWithOptions(options ringbuf.BufferOptions) (*Builder[Req, Res], *History[Req, Res], error) {
	history, err := NewHistoryWithOptions[Req, Res](options)
	if err != nil {
		return b, nil, err
	}
	b.history = history
	b.historyErr = nil
	b.Use(HistoryMiddleware(history))
	return b, history, nil
}

// RecordHistory attaches a ring-buffer execution history to the action.
// If allocation is rejected, Build().Do returns the configuration error before
// the action runs; use RecordHistoryChecked to handle it immediately.
//
// The built action keeps the last capacity records (newest-first via Snapshot).
func (b *Builder[Req, Res]) RecordHistory(capacity int) *Builder[Req, Res] {
	b, _ = b.WithHistory(capacity)
	return b
}

// RecordHistoryChecked is the fluent form of WithHistoryChecked.
func (b *Builder[Req, Res]) RecordHistoryChecked(capacity int) (*Builder[Req, Res], error) {
	builder, _, err := b.WithHistoryChecked(capacity)
	return builder, err
}

// RecordHistoryWithOptions is the fluent form of WithHistoryWithOptions.
func (b *Builder[Req, Res]) RecordHistoryWithOptions(options ringbuf.BufferOptions) (*Builder[Req, Res], error) {
	builder, _, err := b.WithHistoryWithOptions(options)
	return builder, err
}

// LogCalls injects a structured log entry for every call using the provided logger.
// Logs before the call (level=Debug) and after (level=Info on success, Error on failure).
//
// This is a lightweight alternative to telemetry.New() for simple deployments.
//
//	debugAct := action.New("order.create", handler).
//	    LogCalls(slog.Default()).
//	    Build()
func (b *Builder[Req, Res]) LogCalls(log *slog.Logger) *Builder[Req, Res] {
	name := b.meta.Name
	b.trackExecutionStart = true
	return b.HookBefore(func(ctx context.Context, _ Req, _ *Meta) (context.Context, error) {
		log.DebugContext(ctx, "action_start", "action", name)
		return RecordExecutionStart(ctx), nil
	}).HookAfter(func(ctx context.Context, _ Req, _ Res, err error, _ *Meta) {
		dur := ExecutionDuration(ctx)
		if err != nil {
			log.ErrorContext(ctx, "action_error", "action", name, "duration", dur, "error", err)
		} else {
			log.InfoContext(ctx, "action_ok", "action", name, "duration", dur)
		}
	})
}

// Instrument wires a minimal set of Prometheus-style counters via AnyHook.
// Counts are tracked by calling the provided inc functions — no Prometheus
// import required, works with any counter abstraction.
//
// Example with Prometheus:
//
//	calls   := prometheus.NewCounterVec(...)
//	errors  := prometheus.NewCounterVec(...)
//	latency := prometheus.NewHistogramVec(...)
//
//	act := action.New("order.create", handler).
//	    Instrument(
//	        func(name string) { calls.WithLabelValues(name).Inc() },
//	        func(name string) { errors.WithLabelValues(name).Inc() },
//	        func(name string, ms float64) { latency.WithLabelValues(name).Observe(ms / 1000) },
//	    ).Build()
func (b *Builder[Req, Res]) Instrument(
	incCall func(actionName string),
	incError func(actionName string),
	recordLatency func(actionName string, ms float64),
) *Builder[Req, Res] {
	b.trackExecutionStart = true
	return b.AnyHook(AnyHook{
		Before: func(ctx context.Context, _ any, meta *Meta) (context.Context, error) {
			if incCall != nil {
				incCall(meta.Name)
			}
			return RecordExecutionStart(ctx), nil
		},
		After: func(ctx context.Context, _, _ any, err error, meta *Meta) {
			if recordLatency != nil {
				recordLatency(meta.Name, float64(ExecutionDuration(ctx).Milliseconds()))
			}
			if err != nil && incError != nil {
				incError(meta.Name)
			}
		},
	})
}

func SlowLogMiddleware[Req, Res any](threshold time.Duration, actionName string) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			start := time.Now()
			res, err := next(ctx, req)
			if dur := time.Since(start); dur > threshold {
				slog.WarnContext(ctx, "action_slow_execution",
					"action", actionName,
					"duration", dur,
					"threshold", threshold,
				)
			}
			return res, err
		}
	}
}
