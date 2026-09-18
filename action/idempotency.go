// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultIdempotencyTTL      = 24 * time.Hour
	DefaultIdempotencyLeaseTTL = 2 * time.Minute
)

// IdempotencyConfig controls per-action idempotency behavior.
// Zero value = disabled. Attach via .Idempotent() or .IdempotentWithConfig().
type IdempotencyConfig struct {
	// Enabled activates idempotency for this action.
	Enabled bool

	// TTL overrides the store's default TTL for entries of this action.
	// 0 = use store default (24 h).
	TTL time.Duration

	// KeyHeader is the HTTP header to read the key from.
	// Defaults to "Idempotency-Key".
	KeyHeader string

	// KeyFunc derives the idempotency key from the raw request body bytes.
	// Useful when the key lives inside JSON rather than a header.
	// When nil, only KeyHeader is used.
	KeyFunc func(body []byte) string

	// LeaseTTL bounds one in-progress owner when a store provides atomic
	// coordination. Zero uses DefaultIdempotencyLeaseTTL. It must exceed the
	// action's worst-case execution time; it is not the completed replay TTL.
	LeaseTTL time.Duration
}

// Header returns the effective header name (never empty).
func (c IdempotencyConfig) Header() string {
	if c.KeyHeader != "" {
		return c.KeyHeader
	}
	return "Idempotency-Key"
}

// EffectiveLeaseTTL returns the configured lease or the documented default.
func (c IdempotencyConfig) EffectiveLeaseTTL() time.Duration {
	if c.LeaseTTL > 0 {
		return c.LeaseTTL
	}
	return DefaultIdempotencyLeaseTTL
}

// ── Store Contracts ───────────────────────────────────────────────────────────

// IdempotencyEntry is the captured response for a completed idempotent request.
type IdempotencyEntry struct {
	Status      int
	Body        []byte
	Headers     map[string]string // safe headers only: Content-Type, X-Request-ID
	StoredAt    time.Time
	RequestHash string
}

// IdempotencyStore persists and retrieves idempotency entries.
// Implement this interface backed by Redis for multi-node deployments.
// The default MemoryIdempotencyStore is suitable for single-node / dev.
type IdempotencyStore interface {
	Get(ctx context.Context, key string) (IdempotencyEntry, bool)
	Set(ctx context.Context, key string, entry IdempotencyEntry, ttl time.Duration)
}

// IdempotencyClaimState describes one atomic attempt to own an idempotency
// key. It is deliberately business-neutral: transports decide whether an
// in-progress request should be retried, polled, or reported to a caller.
type IdempotencyClaimState uint8

const (
	IdempotencyClaimAcquired IdempotencyClaimState = iota + 1
	IdempotencyClaimCompleted
	IdempotencyClaimInProgress
	IdempotencyClaimConflict
)

// IdempotencyClaim is returned by an atomic coordinator. Token is populated
// only for an acquired claim and must be presented to Complete or Release.
type IdempotencyClaim struct {
	State IdempotencyClaimState
	Token string
	Entry IdempotencyEntry
}

// IdempotencyCoordinator is an optional stronger capability implemented by a
// durable store. Claim must atomically create an in-progress owner or return
// the already stored state. Complete and Release must affect only a claim held
// by the supplied opaque token.
//
// This protects duplicate execution while a valid lease is held. It does not
// make an arbitrary external side effect globally exactly-once; use a
// transactional business write/outbox where that guarantee is required.
type IdempotencyCoordinator interface {
	IdempotencyStore
	Claim(ctx context.Context, key, requestHash string, leaseTTL time.Duration) (IdempotencyClaim, error)
	Complete(ctx context.Context, key, token string, entry IdempotencyEntry, ttl time.Duration) error
	Release(ctx context.Context, key, token string) error
}

type memEntry struct {
	IdempotencyEntry
	ttl time.Duration
	seq uint64
}

type memClaim struct {
	token       string
	requestHash string
	expiresAt   time.Time
}

var (
	_ IdempotencyStore       = (*MemoryIdempotencyStore)(nil)
	_ IdempotencyCoordinator = (*MemoryIdempotencyStore)(nil)
)

// MemoryIdempotencyStore provides an in-memory implementation of IdempotencyCoordinator
// with background TTL eviction and atomic lease claims.
type MemoryIdempotencyStore struct {
	mu        sync.RWMutex
	entries   map[string]memEntry
	claims    map[string]memClaim
	defTTL    time.Duration
	stopCh    chan struct{}
	closeOnce sync.Once
	nextSeq   uint64
}

// NewMemoryIdempotencyStore creates a store with background TTL eviction.
// defTTL 0 → 24 h.
func NewMemoryIdempotencyStore(defTTL time.Duration) *MemoryIdempotencyStore {
	if defTTL == 0 {
		defTTL = DefaultIdempotencyTTL
	}
	s := &MemoryIdempotencyStore{
		entries: make(map[string]memEntry),
		claims:  make(map[string]memClaim),
		defTTL:  defTTL,
		stopCh:  make(chan struct{}),
	}
	go s.evict()
	return s
}

// Close stops the eviction goroutine. Safe to call multiple times.
func (s *MemoryIdempotencyStore) Close() {
	s.closeOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *MemoryIdempotencyStore) Get(_ context.Context, key string) (IdempotencyEntry, bool) {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()

	if !ok {
		return IdempotencyEntry{}, false
	}

	ttl := e.ttl
	if ttl == 0 {
		ttl = s.defTTL
	}

	if time.Since(e.StoredAt) > ttl {
		s.mu.Lock()
		// Double-checked eviction: only delete if this exact sequence entry is still in place.
		if cur, ok := s.entries[key]; ok && cur.seq == e.seq {
			delete(s.entries, key)
		}
		s.mu.Unlock()
		return IdempotencyEntry{}, false
	}

	return e.IdempotencyEntry, true
}

func (s *MemoryIdempotencyStore) setLocked(key string, entry IdempotencyEntry, ttl time.Duration) {
	if entry.StoredAt.IsZero() {
		entry.StoredAt = time.Now()
	}

	s.nextSeq++
	s.entries[key] = memEntry{
		IdempotencyEntry: entry,
		ttl:              ttl,
		seq:              s.nextSeq,
	}
}

func (s *MemoryIdempotencyStore) Set(_ context.Context, key string, entry IdempotencyEntry, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setLocked(key, entry, ttl)
}

func (s *MemoryIdempotencyStore) Claim(ctx context.Context, key, requestHash string, leaseTTL time.Duration) (IdempotencyClaim, error) {
	if err := ctx.Err(); err != nil {
		return IdempotencyClaim{}, err
	}
	if leaseTTL <= 0 {
		leaseTTL = DefaultIdempotencyLeaseTTL
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Check existing completed entry
	if entry, ok := s.entries[key]; ok {
		ttl := entry.ttl
		if ttl == 0 {
			ttl = s.defTTL
		}
		if now.After(entry.StoredAt.Add(ttl)) {
			delete(s.entries, key)
		} else {
			if requestHash != "" && entry.RequestHash != "" && requestHash != entry.RequestHash {
				return IdempotencyClaim{State: IdempotencyClaimConflict}, nil
			}
			return IdempotencyClaim{State: IdempotencyClaimCompleted, Entry: entry.IdempotencyEntry}, nil
		}
	}

	// 2. Check active in-progress lease
	if claim, ok := s.claims[key]; ok {
		if now.Before(claim.expiresAt) {
			if requestHash != "" && claim.requestHash != "" && requestHash != claim.requestHash {
				return IdempotencyClaim{State: IdempotencyClaimConflict}, nil
			}
			return IdempotencyClaim{State: IdempotencyClaimInProgress}, nil
		}
		delete(s.claims, key)
	}

	// 3. Issue new atomic lease claim
	token, err := newClaimToken()
	if err != nil {
		return IdempotencyClaim{}, err
	}

	s.claims[key] = memClaim{
		token:       token,
		requestHash: requestHash,
		expiresAt:   now.Add(leaseTTL),
	}

	return IdempotencyClaim{State: IdempotencyClaimAcquired, Token: token}, nil
}

func (s *MemoryIdempotencyStore) Complete(_ context.Context, key, token string, entry IdempotencyEntry, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	claim, ok := s.claims[key]
	if !ok {
		return errors.New("idempotency: no active claim found for key")
	}
	if claim.token != token {
		return errors.New("idempotency: token does not match active claim")
	}

	delete(s.claims, key)
	s.setLocked(key, entry, ttl)
	return nil
}

func (s *MemoryIdempotencyStore) Release(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	claim, ok := s.claims[key]
	if !ok {
		// Claim was already completed or released
		return nil
	}
	if claim.token != token {
		return errors.New("idempotency: token does not match active claim")
	}

	delete(s.claims, key)
	return nil
}

func (s *MemoryIdempotencyStore) evict() {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			now := time.Now()
			s.mu.Lock()
			for k, e := range s.entries {
				ttl := e.ttl
				if ttl == 0 {
					ttl = s.defTTL
				}
				if now.Sub(e.StoredAt) > ttl {
					delete(s.entries, k)
				}
			}
			for k, c := range s.claims {
				if now.After(c.expiresAt) {
					delete(s.claims, k)
				}
			}
			s.mu.Unlock()
		}
	}
}

func newClaimToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idempotency: failed generating claim token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
