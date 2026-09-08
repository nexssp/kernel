// Package observe provides optional, vendor-neutral action lifecycle observation for Nexss Kernel.
package observe

import (
	"context"
	"time"

	"github.com/nexssp/kernel/action"
)

// Lifecycle kind constants.
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

// Event is a structured observation emitted during an action execution lifecycle.
type Event struct {
	Time        time.Time
	Duration    time.Duration
	Kind        string
	Action      string
	ExecutionID string
	TraceID     string
	SpanID      string
	Request     any
	Response    any
	Error       error
	Recovered   any
	Attempt     int
}

type observationStartKey struct{}

// Sink receives lifecycle events. Implementations must be safe for concurrent use.
type Sink interface {
	Emit(context.Context, Event)
}

// Hook constructs an action.AnyHook that forwards selected lifecycle events to sink.
// Passing a nil sink returns an empty action.AnyHook{}, ensuring zero overhead when disabled.
func Hook(sink Sink) action.AnyHook {
	if sink == nil {
		return action.AnyHook{}
	}
	return action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return context.WithValue(ctx, observationStartKey{}, time.Now()), nil
		},
		OnExecuted: func(ctx context.Context, req, res any, _ error, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindExecuted, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req, Response: res})
		},
		OnError: func(ctx context.Context, req any, err error, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindError, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req, Error: err})
		},
		OnRetry: func(ctx context.Context, req any, attempt int, err error, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindRetry, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req, Error: err, Attempt: attempt})
		},
		OnCacheHit: func(ctx context.Context, req, res any, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindCacheHit, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req, Response: res})
		},
		OnCacheMiss: func(ctx context.Context, req any, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindCacheMiss, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req})
		},
		OnCancel: func(ctx context.Context, req any, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindCanceled, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req})
		},
		OnPanic: func(ctx context.Context, req, recovered any, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindPanic, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req, Recovered: recovered})
		},
		OnCoalesced: func(ctx context.Context, req any, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindCoalesced, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req})
		},
		OnDeduplicated: func(ctx context.Context, req any, meta *action.Meta) {
			sink.Emit(ctx, Event{Time: time.Now(), Duration: durationSince(ctx), Kind: KindDeduplicated, Action: actionName(meta), ExecutionID: action.ExecutionIDFrom(ctx), TraceID: action.TraceIDFrom(ctx), SpanID: action.SpanIDFrom(ctx), Request: req})
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
