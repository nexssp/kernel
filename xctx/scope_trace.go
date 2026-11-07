package xctx

import "context"

// MaxTraceEvents bounds the per-request trace ring. Once full, the oldest
// event is dropped to make room. Exported so callers can size assertions
// and load tests without a magic literal.
const MaxTraceEvents = 128

// AddTrace appends a diagnostic event to the request's trace ring.
//
// Safe for concurrent use. TraceEvents uses a separate lock so single-field
// metadata reads do not contend with trace writes. Paths needing both locks
// must acquire RequestScope.mu before traceMu.
//
// The generation check rejects writes to a scope that has already been
// returned to the pool, so a goroutine holding a stale context cannot
// corrupt a scope now serving a different request.
func AddTrace(ctx context.Context, event string) {
	s := ScopeFrom(ctx)
	if s == nil {
		return
	}
	gen, _ := ctx.Value(scopeGenKeyT{}).(uint64)
	s.traceMu.Lock()
	if s.generation.Load() == gen && gen != 0 {
		if len(s.TraceEvents) >= MaxTraceEvents {
			copy(s.TraceEvents, s.TraceEvents[1:])
			s.TraceEvents = s.TraceEvents[:MaxTraceEvents-1]
		}
		s.TraceEvents = append(s.TraceEvents, event)
	}
	s.traceMu.Unlock()
}
