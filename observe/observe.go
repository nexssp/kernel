package observe

import (
	"context"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

const (
	KindExecuted     = "executed"
	KindError        = "error"
	KindRetry        = "retry"
	KindCacheHit     = "cache_hit"
	KindCacheMiss    = "cache_miss"
	KindCanceled     = "canceled"
	KindPanic        = "panic"
	KindCoalesced    = "coalesced"
	KindDeduplicated = "deduplicated"
)

type Event struct {
	Time        time.Time
	Duration    time.Duration
	Kind        string
	Action      string
	RequestID   string
	ExecutionID string
	TraceID     string
	SpanID      string
	TenantID    string
	UserID      string
	Request     any
	Response    any
	Error       error
	Recovered   any
	Attempt     int
}

type observationStartKey struct{}

type Sink interface {
	Emit(context.Context, Event)
}

func baseEvent(ctx context.Context, kind string, meta *action.Meta) Event {
	return Event{
		Time:        time.Now(),
		Duration:    durationSince(ctx),
		Kind:        kind,
		Action:      actionName(meta),
		RequestID:   xctx.RequestIDFrom(ctx),
		ExecutionID: xctx.ExecutionIDFrom(ctx),
		TraceID:     xctx.TraceIDFrom(ctx),
		SpanID:      xctx.SpanIDFrom(ctx),
		TenantID:    xctx.TenantIDFrom(ctx),
		UserID:      xctx.UserIDFrom(ctx),
	}
}

func Hook(sink Sink) action.AnyHook {
	if sink == nil {
		return action.AnyHook{}
	}
	return action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return context.WithValue(ctx, observationStartKey{}, time.Now()), nil
		},
		OnExecuted: func(ctx context.Context, req, res any, _ error, meta *action.Meta) {
			ev := baseEvent(ctx, KindExecuted, meta)
			ev.Request = req
			ev.Response = res
			sink.Emit(ctx, ev)
		},
		OnError: func(ctx context.Context, req any, err error, meta *action.Meta) {
			ev := baseEvent(ctx, KindError, meta)
			ev.Request = req
			ev.Error = err
			sink.Emit(ctx, ev)
		},
		OnRetry: func(ctx context.Context, req any, attempt int, err error, meta *action.Meta) {
			ev := baseEvent(ctx, KindRetry, meta)
			ev.Request = req
			ev.Error = err
			ev.Attempt = attempt
			sink.Emit(ctx, ev)
		},
		OnCacheHit: func(ctx context.Context, req, res any, meta *action.Meta) {
			ev := baseEvent(ctx, KindCacheHit, meta)
			ev.Request = req
			ev.Response = res
			sink.Emit(ctx, ev)
		},
		OnCacheMiss: func(ctx context.Context, req any, meta *action.Meta) {
			ev := baseEvent(ctx, KindCacheMiss, meta)
			ev.Request = req
			sink.Emit(ctx, ev)
		},
		OnCancel: func(ctx context.Context, req any, meta *action.Meta) {
			ev := baseEvent(ctx, KindCanceled, meta)
			ev.Request = req
			sink.Emit(ctx, ev)
		},
		OnPanic: func(ctx context.Context, req, recovered any, meta *action.Meta) {
			ev := baseEvent(ctx, KindPanic, meta)
			ev.Request = req
			ev.Recovered = recovered
			sink.Emit(ctx, ev)
		},
		OnCoalesced: func(ctx context.Context, req any, meta *action.Meta) {
			ev := baseEvent(ctx, KindCoalesced, meta)
			ev.Request = req
			sink.Emit(ctx, ev)
		},
		OnDeduplicated: func(ctx context.Context, req any, meta *action.Meta) {
			ev := baseEvent(ctx, KindDeduplicated, meta)
			ev.Request = req
			sink.Emit(ctx, ev)
		},
	}
}

func durationSince(ctx context.Context) time.Duration {
	start, ok := ctx.Value(observationStartKey{}).(time.Time)
	if !ok {
		return 0
	}
	return time.Since(start)
}

func actionName(meta *action.Meta) string {
	if meta == nil {
		return ""
	}
	return meta.Name
}
