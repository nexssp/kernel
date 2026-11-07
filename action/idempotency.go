package action

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
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
	// RequestHashBytes is an in-memory fast path; RequestHash remains the
	// serialized compatibility field used by existing stores.
	RequestHashBytes [32]byte `json:"-"`
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

// IdempotencyLeaseRenewer is an optional extension: renews the
// in-flight lease at leaseTTL/3 so a slow handler can't lose it.
type IdempotencyLeaseRenewer interface {
	RenewClaim(ctx context.Context, key, token string, leaseTTL time.Duration) error
}

// idempotentResult carries the request hash through singleflight so
// followers can detect a same-key/different-payload mismatch.
type idempotentResult struct {
	requestHash [32]byte
	value       any
}

// IdempotencyMiddleware collapses concurrent identical requests into a single
// handler execution and replays the stored response for later calls.
//
// The request hash is computed before every store lookup so a reused key with
// a different payload returns Conflict instead of silently replaying the old
// response. Requests that cannot be hashed are rejected before touching the
// store or invoking the handler.
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

			reqHash, hashErr := hashPayloadBytes(req)
			if hashErr != nil {
				var zero Res
				return zero, xerr.BadRequest("idempotency: request payload cannot be hashed", hashErr)
			}

			key := extractIdempotencyKey(ctx, req, cfg, reqHash)
			if key == "" {
				return next(ctx, req)
			}

			ttl := cfg.TTL
			if ttl <= 0 {
				ttl = DefaultIdempotencyTTL
			}

			// Fast path: replay cached result, but only after verifying the
			// payload hash matches. A hash mismatch is a Conflict, not a hit.
			if entry, ok := store.Get(ctx, key); ok {
				if requestHashConflict(entry, reqHash) {
					var zero Res
					return zero, xerr.Conflict("idempotency key already used with a different request payload")
				}
				var cachedRes Res
				if err := json.Unmarshal(entry.Body, &cachedRes); err != nil {
					// Corrupted: fail closed, never re-execute.
					var zero Res
					return zero, xerr.Internal("idempotency: stored response for key is corrupted")
				}
				return cachedRes, nil
			}

			// SingleFlight synchronizes concurrent in-flight executions within the process
			val, err, _ := sf.Do(key, func() (any, error) {
				// Re-check inside flight in case a concurrent caller completed it
				if entry, ok := store.Get(ctx, key); ok {
					if requestHashConflict(entry, reqHash) {
						return nil, xerr.Conflict("idempotency key already used with a different request payload")
					}
					var cachedRes Res
					if err := json.Unmarshal(entry.Body, &cachedRes); err != nil {
						// Corrupted: fail closed.
						return nil, xerr.Internal("idempotency: stored response for key is corrupted")
					}
					return idempotentResult{requestHash: reqHash, value: cachedRes}, nil
				}

				// Handle distributed coordinator if configured
				if coord, ok := store.(IdempotencyCoordinator); ok {
					claim, err := coord.Claim(ctx, key, hashPayloadString(reqHash), cfg.EffectiveLeaseTTL())
					if err != nil {
						if ctxErr := ctx.Err(); ctxErr != nil {
							return nil, ctxErr
						}
						return nil, xerr.Unavailable("idempotency coordination unavailable", err)
					}

					switch claim.State {
					case IdempotencyClaimCompleted:
						var cachedRes Res
						if err := json.Unmarshal(claim.Entry.Body, &cachedRes); err != nil {
							// Corrupted: fail closed.
							return nil, xerr.Internal("idempotency: stored response for key is corrupted")
						}
						return idempotentResult{requestHash: reqHash, value: cachedRes}, nil
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

					// LIFO defer order stops the renewer BEFORE Release.
					stopRenewal := startLeaseRenewal(ctx, coord, key, claim.Token, cfg.EffectiveLeaseTTL())
					defer stopRenewal()

					res, execErr := next(ctx, req)
					if execErr != nil {
						return nil, execErr
					}

					entry, responseErr := idempotencyResponseEntry(ctx, res, reqHash)
					if completeErr := coord.Complete(context.WithoutCancel(ctx), key, claim.Token, entry, ttl); completeErr != nil {
						return nil, xerr.Unavailable("idempotency completion unavailable", completeErr)
					}
					completed = true
					if responseErr != nil {
						return nil, responseErr
					}

					return idempotentResult{requestHash: reqHash, value: res}, nil
				}

				// In-process local execution
				res, execErr := next(ctx, req)
				if execErr != nil {
					return nil, execErr
				}

				entry, responseErr := idempotencyResponseEntry(ctx, res, reqHash)
				store.Set(ctx, key, entry, ttl)
				if responseErr != nil {
					return nil, responseErr
				}

				return idempotentResult{requestHash: reqHash, value: res}, nil
			})

			if err != nil {
				var zero Res
				return zero, err
			}

			fw, ok := val.(idempotentResult)
			if !ok {
				var zero Res
				return zero, xerr.Internal(fmt.Sprintf("idempotency result type mismatch: got %T", val))
			}
			// Followers never run the flight function — the only place
			// a payload mismatch can be caught.
			if fw.requestHash != ([32]byte{}) && reqHash != ([32]byte{}) && fw.requestHash != reqHash {
				var zero Res
				return zero, xerr.Conflict("idempotency key already used with a different request payload")
			}
			res, ok := fw.value.(Res)
			if !ok {
				var zero Res
				return zero, xerr.Internal(fmt.Sprintf("idempotency result type mismatch: got %T", fw.value))
			}
			return res, nil
		}
	}
}

// idempotencyResponseEntry always returns an entry so a successful handler
// execution is never forgotten merely because its response cannot be encoded.
// The invalid JSON marker makes subsequent requests fail closed rather than
// repeating the handler's side effects.
func idempotencyResponseEntry(ctx context.Context, res any, requestHash [32]byte) (IdempotencyEntry, error) {
	body, err := json.Marshal(res)
	if err != nil {
		slog.WarnContext(ctx, "idempotency_response_marshal_failed")
		body = []byte("!idempotency response unavailable")
	}
	entry := IdempotencyEntry{
		Status:           200,
		Body:             body,
		StoredAt:         time.Now(),
		RequestHash:      hashPayloadString(requestHash),
		RequestHashBytes: requestHash,
	}
	if err != nil {
		return entry, xerr.Internal("idempotency: successful response cannot be serialized for replay", err)
	}
	return entry, nil
}

func extractIdempotencyKey[Req any](ctx context.Context, req Req, cfg IdempotencyConfig, requestHash [32]byte) string {
	if requestID := xctx.RequestIDFrom(ctx); requestID != "" {
		return requestID
	}
	if b, ok := any(req).([]byte); ok && cfg.KeyFunc != nil {
		return cfg.KeyFunc(b)
	}
	return hashPayloadString(requestHash)
}

func hashPayloadString(sum [32]byte) string {
	if sum == ([32]byte{}) {
		return ""
	}
	return hex.EncodeToString(sum[:])
}

func requestHashConflict(entry IdempotencyEntry, requestHash [32]byte) bool {
	if requestHash == ([32]byte{}) || (entry.RequestHash == "" && entry.RequestHashBytes == ([32]byte{})) {
		return false
	}
	stored := entry.RequestHashBytes
	if stored == ([32]byte{}) {
		stored = decodeRequestHash(entry.RequestHash)
	}
	return stored != requestHash
}

func decodeRequestHash(encoded string) [32]byte {
	var decoded [32]byte
	if len(encoded) != hex.EncodedLen(len(decoded)) {
		return decoded
	}
	for i := range decoded {
		hi, okHi := hexNibble(encoded[i*2])
		lo, okLo := hexNibble(encoded[i*2+1])
		if !okHi || !okLo {
			return [32]byte{}
		}
		decoded[i] = hi<<4 | lo
	}
	return decoded
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

// hashPayloadBytes returns the raw [32]byte SHA-256 of the request payload,
// or an error if the payload cannot be serialized.
// Zero-allocation for primitive types. Falls back to json.Marshal for
// complex types (which allocates).
//
// Direct byte comparison via [32]byte is the preferred hot-path API:
//
//	if entry.RequestHashBytes != [32]byte{} && reqHashBytes != [32]byte{} &&
//	    entry.RequestHashBytes != reqHashBytes { ... conflict }
func hashPayloadBytes(v any) ([32]byte, error) {
	switch x := v.(type) {
	case nil:
		return sha256.Sum256([]byte("\x00nil\x00")), nil
	case string:
		return sha256.Sum256([]byte(x)), nil // ← Go runtime: zero-alloc for string→[]byte in this context
	case []byte:
		return sha256.Sum256(x), nil
	case int:
		// strconv.AppendInt reuses a stack buffer via strconv.Itoa fast path.
		buf := make([]byte, 0, 20)
		buf = strconv.AppendInt(buf, int64(x), 10)
		return sha256.Sum256(buf), nil
	case int64:
		buf := make([]byte, 0, 20)
		buf = strconv.AppendInt(buf, x, 10)
		return sha256.Sum256(buf), nil
	case uint:
		buf := make([]byte, 0, 20)
		buf = strconv.AppendUint(buf, uint64(x), 10)
		return sha256.Sum256(buf), nil
	case uint64:
		buf := make([]byte, 0, 20)
		buf = strconv.AppendUint(buf, x, 10)
		return sha256.Sum256(buf), nil
	}
	if hasTextMarshalerMapKey(reflect.ValueOf(v), make(map[idempotencyMapVisit]struct{})) {
		return [32]byte{}, errors.New("idempotency: TextMarshaler map keys are not supported")
	}
	// Fallback for complex types — uses encoding/json which allocates.
	b, err := json.Marshal(v)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal request payload: %w", err)
	}
	return sha256.Sum256(b), nil
}

type idempotencyMapVisit struct {
	typeOf  reflect.Type
	pointer uintptr
	length  int
}

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
)

func hasTextMarshalerMapKey(value reflect.Value, seen map[idempotencyMapVisit]struct{}) bool {
	if !value.IsValid() || hasJSONMarshaler(value) {
		return false
	}
	switch value.Kind() {
	case reflect.Interface:
		return !value.IsNil() && hasTextMarshalerMapKey(value.Elem(), seen)
	case reflect.Pointer:
		return !value.IsNil() && markIdempotencyValue(value, seen) && hasTextMarshalerMapKey(value.Elem(), seen)
	case reflect.Map:
		return !value.IsNil() && markIdempotencyValue(value, seen) && mapHasTextMarshalerKeys(value, seen)
	case reflect.Slice:
		return !value.IsNil() && markIdempotencyValue(value, seen) && sequenceHasTextMarshalerKeys(value, seen)
	case reflect.Array:
		return sequenceHasTextMarshalerKeys(value, seen)
	case reflect.Struct:
		return structHasTextMarshalerKeys(value, seen)
	// Scalar and unsupported kinds cannot contain nested map keys.
	case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func,
		reflect.String, reflect.UnsafePointer:
		return false
	default:
		return false
	}
}

func markIdempotencyValue(value reflect.Value, seen map[idempotencyMapVisit]struct{}) bool {
	visit := idempotencyMapVisit{typeOf: value.Type(), pointer: value.Pointer()}
	if value.Kind() == reflect.Slice {
		visit.length = value.Len()
	}
	if _, ok := seen[visit]; ok {
		return false
	}
	seen[visit] = struct{}{}
	return true
}

func mapHasTextMarshalerKeys(value reflect.Value, seen map[idempotencyMapVisit]struct{}) bool {
	keyType := value.Type().Key()
	if keyType.Kind() != reflect.String && keyType.Implements(textMarshalerType) {
		return true
	}
	iterator := value.MapRange()
	for iterator.Next() {
		if hasTextMarshalerMapKey(iterator.Value(), seen) {
			return true
		}
	}
	return false
}

func sequenceHasTextMarshalerKeys(value reflect.Value, seen map[idempotencyMapVisit]struct{}) bool {
	for index := range value.Len() {
		if hasTextMarshalerMapKey(value.Index(index), seen) {
			return true
		}
	}
	return false
}

func structHasTextMarshalerKeys(value reflect.Value, seen map[idempotencyMapVisit]struct{}) bool {
	for index := range value.NumField() {
		field := value.Type().Field(index)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		if tag, ok := field.Tag.Lookup("json"); ok {
			name, _, _ := strings.Cut(tag, ",")
			if name == "-" {
				continue
			}
		}
		if hasTextMarshalerMapKey(value.Field(index), seen) {
			return true
		}
	}
	return false
}

func hasJSONMarshaler(value reflect.Value) bool {
	typeOf := value.Type()
	if typeOf.Implements(jsonMarshalerType) {
		return true
	}
	// TextMarshaler affects map keys only; those are checked directly above.
	return value.CanAddr() && value.Addr().Type().Implements(jsonMarshalerType)
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
	if entry.RequestHashBytes == ([32]byte{}) {
		entry.RequestHashBytes = decodeRequestHash(entry.RequestHash)
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
	for k := range s.entries {
		ttl := s.entries[k].ttl
		if ttl <= 0 {
			ttl = s.defTTL
		}
		if now.Sub(s.entries[k].StoredAt) > ttl {
			delete(s.entries, k)
		}
	}
	for k, c := range s.claims {
		if now.After(c.expiresAt) {
			delete(s.claims, k)
		}
	}
}

// startLeaseRenewal renews at leaseTTL/3 until stop is called.
// No-op if the store doesn't implement IdempotencyLeaseRenewer.
func startLeaseRenewal(
	ctx context.Context,
	coord IdempotencyCoordinator,
	key, token string,
	leaseTTL time.Duration,
) func() {
	renewer, ok := coord.(IdempotencyLeaseRenewer)
	if !ok || leaseTTL <= 0 {
		return func() {}
	}

	interval := leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				rCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
				renewErr := renewer.RenewClaim(rCtx, key, token, leaseTTL)
				cancel()
				if renewErr != nil {
					select {
					case <-stop:
						return // shutdown race with Release — not an error
					default:
					}
					slog.WarnContext(ctx, "idempotency_lease_renewal_failed",
						"key", key, "error", renewErr)
					return
				}
			}
		}
	}()
	return func() { close(stop) }
}

func newClaimToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("idempotency: failed generating claim token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// RenewClaim extends the lease of an in-flight claim. Implements the optional
// IdempotencyLeaseRenewer interface so IdempotencyMiddleware keeps long-running
// handlers from losing their lease to expiry.
func (s *MemoryIdempotencyStore) RenewClaim(_ context.Context, key, token string, leaseTTL time.Duration) error {
	if leaseTTL <= 0 {
		leaseTTL = DefaultIdempotencyLeaseTTL
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	claim, ok := s.claims[key]
	if !ok {
		return errors.New("idempotency: no active claim found for key")
	}
	if claim.token != token {
		return errors.New("idempotency: token does not match active claim")
	}

	// claims stores values: mutate the copy and write it back.
	claim.expiresAt = time.Now().Add(leaseTTL)
	s.claims[key] = claim
	return nil
}
