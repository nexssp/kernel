package action

// builder_observe.go — Observability shortcuts on Builder[Req, Res].
//
// These methods wrap common hook patterns to keep call sites concise
// while preserving full type safety.

import (
	"context"
	"log/slog"
	"time"
)

// WithHistory attaches a ring-buffer execution history to the action
// AND returns the *History[Req, Res] handle for inspection.
// Replaces the two-return-value .WithHistory pattern with a single
// method that stores the handle on the builder for later retrieval.
//
// The built action keeps the last cap records (newest-first via Snapshot).
//
// Usage:
//
//	paymentAct := action.New("payment.charge", chargeCard).
//	    RecordHistory(200).
//	    Build()
//
//	hist := paymentAct.History() // nil if RecordHistory was not called
func (b *Builder[Req, Res]) RecordHistory(capacity int) *Builder[Req, Res] {
	hist := NewHistory[Req, Res](capacity)
	b.history = hist // stored on builder, transferred to BuiltAction at Build()
	return b.Use(HistoryMiddleware(hist))
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
	return b.HookBefore(func(ctx context.Context, _ Req, _ *Meta) (context.Context, error) {
		log.DebugContext(ctx, "action_start", "action", name)
		return context.WithValue(ctx, logStartKey{}, time.Now()), nil
	}).HookAfter(func(ctx context.Context, _ Req, _ Res, err error, _ *Meta) {
		start, ok := ctx.Value(logStartKey{}).(time.Time)
		if !ok {
			start = time.Now()
		}
		dur := time.Since(start)
		if err != nil {
			log.ErrorContext(ctx, "action_error", "action", name, "duration", dur, "error", err)
		} else {
			log.InfoContext(ctx, "action_ok", "action", name, "duration", dur)
		}
	})
}

type logStartKey struct{}

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
	return b.AnyHook(AnyHook{
		Before: func(ctx context.Context, _ any, meta *Meta) (context.Context, error) {
			if incCall != nil {
				incCall(meta.Name)
			}
			return context.WithValue(ctx, instrumentStartKey{}, time.Now()), nil
		},
		After: func(ctx context.Context, _, _ any, err error, meta *Meta) {
			start, ok := ctx.Value(instrumentStartKey{}).(time.Time)
			if !ok {
				start = time.Now()
			}
			if recordLatency != nil {
				recordLatency(meta.Name, float64(time.Since(start).Milliseconds()))
			}
			if err != nil && incError != nil {
				incError(meta.Name)
			}
		},
	})
}

type instrumentStartKey struct{}

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
