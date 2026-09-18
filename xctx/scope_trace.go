package xctx

import "context"

// MaxTraceEvents bounds the per-request trace ring. Once full, the oldest
// event is dropped to make room. Exported so callers can size assertions
// and load tests without a magic literal.
const MaxTraceEvents = 128

// AddTrace appends a diagnostic event to the request's trace ring.
//
// Safe for concurrent use: it is the only method that mutates TraceEvents
// while the request is in flight. Contention is limited to concurrent
// PublishEvent failures on the same request — an error path, not a hot
// path — so a plain mutex is the correct tool. A lock-free ring would
// require either a 128-bit atomic (no stdlib support) or per-slot
// allocations, both of which lose on the actual workload.
func AddTrace(ctx context.Context, event string) {
	s := ScopeFrom(ctx)
	if s == nil {
		return
	}
	gen, _ := ctx.Value(scopeGenKeyT{}).(uint64)
	s.traceMu.Lock()
	if s.generation == gen && gen != 0 {
		if len(s.TraceEvents) >= MaxTraceEvents {
			copy(s.TraceEvents, s.TraceEvents[1:])
			s.TraceEvents = s.TraceEvents[:MaxTraceEvents-1]
		}
		s.TraceEvents = append(s.TraceEvents, event)
	}
	s.traceMu.Unlock()
}
