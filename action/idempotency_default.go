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

var _ IdempotencyStore = (*MemoryIdempotencyStore)(nil)
var _ IdempotencyCoordinator = (*MemoryIdempotencyStore)(nil)

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
