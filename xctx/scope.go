package xctx

import (
	"context"
	"sync"
	"sync/atomic"
)

type (
	scopeKeyT    struct{}
	scopeGenKeyT struct{}
)

var globalGeneration atomic.Uint64

// RequestScope carries per-request metadata.
type RequestScope struct {
	generation      atomic.Uint64
	RequestID       string
	ExecutionID     string
	RootExecutionID string
	TraceID         string
	SpanID          string
	Endpoint        string
	UserID          string
	TenantID        string
	Role            string
	ClientIP        string
	Roles           []string
	Permissions     []string
	Features        []string
	TraceEvents     []string

	// traceMu guards TraceEvents. Held only by AddTrace. All other
	// readers and writers of TraceEvents run after the request has
	// fully unwound (Reset, CloneForAsync) and take no lock.
	traceMu sync.Mutex
}

// Reset clears every field, retaining slice capacity up to the trace
// ring size. Called by the pool before returning a scope for reuse. It
// does not touch generation; that is the pool's responsibility.
func (s *RequestScope) Reset() {
	s.RequestID = ""
	s.ExecutionID = ""
	s.RootExecutionID = ""
	s.TraceID = ""
	s.SpanID = ""
	s.Endpoint = ""
	s.UserID = ""
	s.TenantID = ""
	s.Role = ""
	s.ClientIP = ""
	s.Roles = s.Roles[:0]
	s.Permissions = s.Permissions[:0]
	s.Features = s.Features[:0]
	if cap(s.TraceEvents) > MaxTraceEvents {
		s.TraceEvents = nil
	} else {
		s.TraceEvents = s.TraceEvents[:0]
	}
}

var scopePool = sync.Pool{New: func() any { return &RequestScope{} }}

// NewScope returns a pooled RequestScope bound to parent, plus a cleanup
// function that returns the scope to the pool. cleanup is idempotent.
func NewScope(parent context.Context) (context.Context, *RequestScope, func()) {
	if parent == nil {
		parent = context.Background()
	}
	s, ok := scopePool.Get().(*RequestScope)
	if !ok || s == nil {
		s = &RequestScope{}
	}
	gen := globalGeneration.Add(1)
	s.generation.Store(gen)
	ctx := context.WithValue(parent, scopeKeyT{}, s)
	ctx = context.WithValue(ctx, scopeGenKeyT{}, gen)
	var once sync.Once
	return ctx, s, func() {
		once.Do(func() {
			s.generation.Store(0)
			s.Reset()
			scopePool.Put(s)
		})
	}
}

// ScopeFrom returns the RequestScope bound to ctx, or nil.
func ScopeFrom(ctx context.Context) *RequestScope {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(scopeKeyT{}).(*RequestScope)
	return s
}

// WithScope binds s to ctx directly. Prefer NewScope for pooled scopes.
//
// Safe for concurrent use on a fresh (never-bound) scope: two goroutines
// racing to bind the same fresh scope will agree on a single generation.
// Once a scope is bound, callers must not bind it to a second context
// concurrently — that is a programming error, not a race the runtime
// can detect.
func WithScope(ctx context.Context, s *RequestScope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	gen := s.generation.Load()
	if gen == 0 {
		candidate := globalGeneration.Add(1)
		if s.generation.CompareAndSwap(0, candidate) {
			gen = candidate
		} else {
			gen = s.generation.Load()
		}
	}
	ctx = context.WithValue(ctx, scopeKeyT{}, s)
	return context.WithValue(ctx, scopeGenKeyT{}, gen)
}

// ensureScope returns ctx with a RequestScope bound. If one is already
// present, it is returned unchanged; otherwise a fresh non-pooled scope
// is created. Used by the With* setters so callers never need to call
// NewScope first.
func ensureScope(ctx context.Context) (context.Context, *RequestScope) {
	if ctx == nil {
		ctx = context.Background()
	}
	s, _ := ctx.Value(scopeKeyT{}).(*RequestScope)
	if s != nil {
		return ctx, s
	}
	s = &RequestScope{}
	gen := globalGeneration.Add(1)
	s.generation.Store(gen)
	ctx = context.WithValue(ctx, scopeKeyT{}, s)
	ctx = context.WithValue(ctx, scopeGenKeyT{}, gen)
	return ctx, s
}
