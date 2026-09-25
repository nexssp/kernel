package action

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"golang.org/x/sync/singleflight"
)

const (
	DefaultIdempotencyTTL      = 24 * time.Hour
	DefaultIdempotencyLeaseTTL = 2 * time.Minute
)

type IdempotencyConfig struct {
	Enabled   bool
	TTL       time.Duration
	KeyHeader string
	KeyFunc   func(body []byte) string
	LeaseTTL  time.Duration
	Store     IdempotencyStore
}

func (c IdempotencyConfig) Header() string {
	if c.KeyHeader != "" {
		return c.KeyHeader
	}
	return "Idempotency-Key"
}

func (c IdempotencyConfig) EffectiveLeaseTTL() time.Duration {
	if c.LeaseTTL > 0 {
		return c.LeaseTTL
	}
	return DefaultIdempotencyLeaseTTL
}

type IdempotencyEntry struct {
	Status      int               `json:"status"`
	Body        []byte            `json:"body"`
	Headers     map[string]string `json:"headers,omitempty"`
	StoredAt    time.Time         `json:"stored_at"`
	RequestHash string            `json:"request_hash"`
}

type IdempotencyStore interface {
	Get(ctx context.Context, key string) (IdempotencyEntry, bool)
	Set(ctx context.Context, key string, entry IdempotencyEntry, ttl time.Duration)
}

type IdempotencyClaimState uint8

const (
	IdempotencyClaimAcquired IdempotencyClaimState = iota + 1
	IdempotencyClaimCompleted
	IdempotencyClaimInProgress
	IdempotencyClaimConflict
)

type IdempotencyClaim struct {
	State IdempotencyClaimState
	Token string
	Entry IdempotencyEntry
}

type IdempotencyCoordinator interface {
	IdempotencyStore
	Claim(ctx context.Context, key, requestHash string, leaseTTL time.Duration) (IdempotencyClaim, error)
	Complete(ctx context.Context, key, token string, entry IdempotencyEntry, ttl time.Duration) error
	Release(ctx context.Context, key, token string) error
}

// IdempotencyMiddleware collapses concurrent identical requests into a single
// handler execution and replays the stored response for later calls.
//
// The request hash is computed before every store lookup so a reused key with
// a different payload returns Conflict instead of silently replaying the old
// response.
//
//nolint:gocyclo // the branches encode the idempotency claim state machine.
func IdempotencyMiddleware[Req, Res any](store IdempotencyStore, cfg IdempotencyConfig) Middleware[Req, Res] {
	if store == nil {
		store = NewMemoryIdempotencyStore(cfg.TTL)
	}

	var sf singleflight.Group

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !cfg.Enabled {
				return next(ctx, req)
			}

			key := extractIdempotencyKey(ctx, req, cfg)
			if key == "" {
				return next(ctx, req)
			}

			reqHash := hashPayload(req)
			ttl := cfg.TTL
			if ttl <= 0 {
				ttl = DefaultIdempotencyTTL
			}

			// Fast path: replay cached result, but only after verifying the
			// payload hash matches. A hash mismatch is a Conflict, not a hit.
			if entry, ok := store.Get(ctx, key); ok {
				if entry.RequestHash != "" && reqHash != "" && entry.RequestHash != reqHash {
					var zero Res
					return zero, xerr.Conflict("idempotency key already used with a different request payload")
				}
				var cachedRes Res
				if err := json.Unmarshal(entry.Body, &cachedRes); err == nil {
					return cachedRes, nil
				}
			}

			// SingleFlight synchronizes concurrent in-flight executions within the process
			val, err, _ := sf.Do(key, func() (any, error) {
				// Re-check inside flight in case a concurrent caller completed it
				if entry, ok := store.Get(ctx, key); ok {
					if entry.RequestHash != "" && reqHash != "" && entry.RequestHash != reqHash {
						return nil, xerr.Conflict("idempotency key already used with a different request payload")
					}
					var cachedRes Res
					if err := json.Unmarshal(entry.Body, &cachedRes); err == nil {
						return cachedRes, nil
					}
				}

				// Handle distributed coordinator if configured
				if coord, ok := store.(IdempotencyCoordinator); ok {
					claim, err := coord.Claim(ctx, key, reqHash, cfg.EffectiveLeaseTTL())
					if err != nil {
						if ctxErr := ctx.Err(); ctxErr != nil {
							return nil, ctxErr
						}
						return nil, xerr.Unavailable("idempotency coordination unavailable", err)
					}

					switch claim.State {
					case IdempotencyClaimCompleted:
						var cachedRes Res
						if err := json.Unmarshal(claim.Entry.Body, &cachedRes); err == nil {
							return cachedRes, nil
						}
					case IdempotencyClaimConflict:
						return nil, xerr.Conflict("idempotency key already used with a different request payload")
					case IdempotencyClaimInProgress:
						return nil, xerr.Conflict("an identical request is currently in progress")
					case IdempotencyClaimAcquired:
					}

					var completed bool
					defer func() {
						if !completed {
							if releaseErr := coord.Release(context.WithoutCancel(ctx), key, claim.Token); releaseErr != nil {
								slog.WarnContext(ctx, "idempotency_claim_release_failed", "key", key, "error", releaseErr)
							}
						}
					}()

					res, execErr := next(ctx, req)
					if execErr != nil {
						return nil, execErr
					}

					if bodyBytes, mErr := json.Marshal(res); mErr == nil {
						entry := IdempotencyEntry{
							Status:      200,
							Body:        bodyBytes,
							StoredAt:    time.Now().UTC(),
							RequestHash: reqHash,
						}
						if completeErr := coord.Complete(context.WithoutCancel(ctx), key, claim.Token, entry, ttl); completeErr != nil {
							return nil, xerr.Unavailable("idempotency completion unavailable", completeErr)
						}
						completed = true
					}

					return res, nil
				}

				// In-process local execution
				res, execErr := next(ctx, req)
				if execErr != nil {
					return nil, execErr
				}

				if bodyBytes, mErr := json.Marshal(res); mErr == nil {
					store.Set(ctx, key, IdempotencyEntry{
						Status:      200,
						Body:        bodyBytes,
						StoredAt:    time.Now().UTC(),
						RequestHash: reqHash,
					}, ttl)
				}

				return res, nil
			})

			if err != nil {
				var zero Res
				return zero, err
			}

			res, ok := val.(Res)
			if !ok {
				var zero Res
				return zero, xerr.Internal(fmt.Sprintf("idempotency result type mismatch: got %T", val))
			}
			return res, nil
		}
	}
}

func extractIdempotencyKey[Req any](ctx context.Context, req Req, cfg IdempotencyConfig) string {
	if s := xctx.ScopeFrom(ctx); s != nil && s.RequestID != "" {
		return s.RequestID
	}
	if b, ok := any(req).([]byte); ok && cfg.KeyFunc != nil {
		return cfg.KeyFunc(b)
	}
	return hashPayload(req)
}

// hashPayload computes a SHA-256 hash of the request payload for
// idempotency-key collision detection.
//
// Hot path: for common primitive types (int, int64, uint, uint64, string,
// []byte), this is zero-allocation — the hash is computed on stack buffers.
// For complex types, it falls back to encoding/json (which allocates).
//
// The returned string is a hex-encoded SHA-256. To eliminate the final
// string allocation, callers can use bytes.Equal on the raw [32]byte
// (see hashPayloadBytes below).
func hashPayload(v any) string {
	sum := hashPayloadBytes(v)
	if sum == ([32]byte{}) {
		return ""
	}
	return hex.EncodeToString(sum[:])
}

// hashPayloadBytes returns the raw [32]byte SHA-256 of the request payload.
// Zero-allocation for primitive types. Falls back to json.Marshal for
// complex types (which allocates).
//
// Direct byte comparison via [32]byte is the preferred hot-path API:
//
//	if entry.RequestHashBytes != [32]byte{} && reqHashBytes != [32]byte{} &&
//	    entry.RequestHashBytes != reqHashBytes { ... conflict }
func hashPayloadBytes(v any) [32]byte {
	switch x := v.(type) {
	case nil:
		return [32]byte{}
	case string:
		return sha256.Sum256([]byte(x)) // ← Go runtime: zero-alloc for string→[]byte in this context
	case []byte:
		return sha256.Sum256(x)
	case int:
		// strconv.AppendInt reuses a stack buffer via strconv.Itoa fast path.
		buf := make([]byte, 0, 20)
		buf = strconv.AppendInt(buf, int64(x), 10)
		return sha256.Sum256(buf)
	case int64:
		buf := make([]byte, 0, 20)
		buf = strconv.AppendInt(buf, x, 10)
		return sha256.Sum256(buf)
	case uint:
		buf := make([]byte, 0, 20)
		buf = strconv.AppendUint(buf, uint64(x), 10)
		return sha256.Sum256(buf)
	case uint64:
		buf := make([]byte, 0, 20)
		buf = strconv.AppendUint(buf, x, 10)
		return sha256.Sum256(buf)
	}
	// Fallback for complex types — uses json.Marshal which allocates.
	b, err := json.Marshal(v)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(b)
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

type MemoryIdempotencyStore struct {
	mu          sync.RWMutex
	entries     map[string]memEntry
	claims      map[string]memClaim
	defTTL      time.Duration
	maxCapacity int
	nextSeq     uint64
}

func NewMemoryIdempotencyStore(defTTL time.Duration) *MemoryIdempotencyStore {
	if defTTL <= 0 {
		defTTL = DefaultIdempotencyTTL
	}
	return &MemoryIdempotencyStore{
		entries:     make(map[string]memEntry),
		claims:      make(map[string]memClaim),
		defTTL:      defTTL,
		maxCapacity: defaultCacheCapacity,
	}
}

func (s *MemoryIdempotencyStore) Get(_ context.Context, key string) (IdempotencyEntry, bool) {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()

	if !ok {
		return IdempotencyEntry{}, false
	}

	ttl := e.ttl
	if ttl <= 0 {
		ttl = s.defTTL
	}

	if time.Since(e.StoredAt) > ttl {
		s.mu.Lock()
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

	if len(s.entries) >= s.maxCapacity {
		s.evictExpiredLocked(time.Now())
		if len(s.entries) >= s.maxCapacity {
			for k := range s.entries {
				delete(s.entries, k)
				if len(s.entries) < s.maxCapacity {
					break
				}
			}
		}
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

	if entry, ok := s.entries[key]; ok {
		ttl := entry.ttl
		if ttl <= 0 {
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

	if claim, ok := s.claims[key]; ok {
		if now.Before(claim.expiresAt) {
			if requestHash != "" && claim.requestHash != "" && requestHash != claim.requestHash {
				return IdempotencyClaim{State: IdempotencyClaimConflict}, nil
			}
			return IdempotencyClaim{State: IdempotencyClaimInProgress}, nil
		}
		delete(s.claims, key)
	}

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
		return nil
	}
	if claim.token != token {
		return errors.New("idempotency: token does not match active claim")
	}

	delete(s.claims, key)
	return nil
}

func (s *MemoryIdempotencyStore) evictExpiredLocked(now time.Time) {
	for k, e := range s.entries {
		ttl := e.ttl
		if ttl <= 0 {
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
}

func newClaimToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idempotency: failed generating claim token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
