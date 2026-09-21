package ktest

import (
	"context"
	"sync"

	"github.com/nexssp/kernel/action"
)

var _ action.HookDispatcher[int, string] = (*Recorder[int, string])(nil)

// Recorder implements action.HookDispatcher with full goroutine concurrency safety.
type Recorder[Req, Res any] struct {
	mu           sync.RWMutex
	Retries      []RetryEvent[Req]
	CacheHits    int
	CacheMisses  int
	Coalesced    int
	Deduplicated int
}

// RetryEvent records one retry attempt.
type RetryEvent[Req any] struct {
	Request Req
	Attempt int
	Err     error
}

func (r *Recorder[Req, Res]) OnCacheHit(context.Context, Req, Res) {
	r.mu.Lock()
	r.CacheHits++
	r.mu.Unlock()
}

func (r *Recorder[Req, Res]) OnCacheMiss(context.Context, Req) {
	r.mu.Lock()
	r.CacheMisses++
	r.mu.Unlock()
}

func (r *Recorder[Req, Res]) OnRetry(_ context.Context, req Req, attempt int, err error) {
	r.mu.Lock()
	r.Retries = append(r.Retries, RetryEvent[Req]{req, attempt, err})
	r.mu.Unlock()
}

func (r *Recorder[Req, Res]) OnCoalesced(context.Context, Req) {
	r.mu.Lock()
	r.Coalesced++
	r.mu.Unlock()
}

func (r *Recorder[Req, Res]) OnDeduplicated(context.Context, Req) {
	r.mu.Lock()
	r.Deduplicated++
	r.mu.Unlock()
}
