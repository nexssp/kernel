package action

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
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

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

const (
	DefaultIdempotencyTTL                  = 24 * time.Hour
	DefaultIdempotencyLeaseTTL             = 2 * time.Minute
	DefaultIdempotencyFlightTimeout        = 2 * time.Minute
	DefaultIdempotencyMaxConcurrentFlights = 256
	idempotencyStorageKeyPrefix            = "kernel:idempotency:v2:"
)

type IdempotencyConfig struct {
	Enabled   bool
	TTL       time.Duration
	KeyHeader string
	Namespace string
	// KeyFunc is the legacy []byte request key derivation. It is called only
	// when the request is []byte and RequestKeyFunc is nil. For typed requests,
	// use RequestKeyFunc.
	KeyFunc              func(body []byte) string          `json:"-"`
	RequestKeyFunc       func(request any) (string, error) `json:"-"`
	MaxConcurrentFlights int
	LeaseTTL             time.Duration
	FlightTimeout        time.Duration
	Store                IdempotencyStore `json:"-"`
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

// EffectiveFlightTimeout returns the configured shared execution limit.
func (c IdempotencyConfig) EffectiveFlightTimeout() time.Duration {
	if c.FlightTimeout > 0 {
		return c.FlightTimeout
	}
	return DefaultIdempotencyFlightTimeout
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

// idempotentResult carries the request hash through a shared flight so
// followers can detect a same-key/different-payload mismatch.
type idempotentResult[Res any] struct {
	requestHash [32]byte
	value       Res
}

type idempotencyFlight struct {
	done  chan struct{}
	value any
	err   error
}

// idempotencyFlightGroup bounds active unique keys. The first caller executes
// synchronously; followers can stop waiting without canceling shared work.
type idempotencyFlightGroup struct {
	mu      sync.Mutex
	flights map[string]*idempotencyFlight
	max     int
}

func newIdempotencyFlightGroup(maxFlights int) *idempotencyFlightGroup {
	if maxFlights <= 0 {
		maxFlights = DefaultIdempotencyMaxConcurrentFlights
	}
	return &idempotencyFlightGroup{flights: make(map[string]*idempotencyFlight), max: maxFlights}
}

func (g *idempotencyFlightGroup) Do(ctx context.Context, key string, fn func(context.Context) (any, error)) (any, error) {
	if ctx == nil {
		return nil, xerr.Validation("idempotency: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	flight := g.flights[key]
	if flight != nil {
		g.mu.Unlock()
		select {
		case <-flight.done:
			return flight.value, flight.err
		case <-ctx.Done():
			select {
			case <-flight.done:
				return flight.value, flight.err
			default:
				return nil, ctx.Err()
			}
		}
	}
	if len(g.flights) >= g.max {
		g.mu.Unlock()
		return nil, xerr.Unavailable("idempotency: concurrent flight capacity reached")
	}
	flight = &idempotencyFlight{done: make(chan struct{})}
	g.flights[key] = flight
	g.mu.Unlock()

	return g.run(ctx, key, flight, fn)
}

func (g *idempotencyFlightGroup) run(ctx context.Context, key string, flight *idempotencyFlight, fn func(context.Context) (any, error)) (value any, err error) {
	returned := false
	defer func() {
		if !returned {
			if recovered := recover(); recovered != nil {
				err = xerr.Internal("idempotency: shared execution panicked", fmt.Errorf("%v", recovered))
			} else {
				err = xerr.Internal("idempotency: shared execution exited without returning")
			}
			value = nil
		}
		g.mu.Lock()
		flight.value = value
		flight.err = err
		if g.flights[key] == flight {
			delete(g.flights, key)
		}
		close(flight.done)
		g.mu.Unlock()
	}()

	value, err = fn(ctx)
	returned = true
	return value, err
}

// IdempotencyMiddleware replays successful responses only when a caller provides
// an explicit idempotency key. Request IDs are for correlation and payload hashes
// detect conflicts; neither is used as a key. Shared work keeps the first caller's
// context values but not its cancellation; each caller can stop waiting using its
// own context. FlightTimeout bounds the shared execution.
//
// A direct middleware must set cfg.Namespace. Builder-created middleware uses the
// final action name unless cfg.Namespace explicitly overrides it. Transport
// adapters should read cfg.Header() and bind the value with xctx.WithIdempotencyKey.
//
//nolint:gocyclo // the branches encode the idempotency claim state machine.
func IdempotencyMiddleware[Req, Res any](store IdempotencyStore, cfg IdempotencyConfig) Middleware[Req, Res] {
	if store == nil {
		store = cfg.Store
	}
	if store == nil {
		store = NewMemoryIdempotencyStore(cfg.TTL)
	}
	maxFlights := cfg.MaxConcurrentFlights
	if maxFlights == 0 {
		maxFlights = DefaultIdempotencyMaxConcurrentFlights
	}
	flightTimeout := cfg.EffectiveFlightTimeout()
	flights := newIdempotencyFlightGroup(maxFlights)

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !cfg.Enabled {
				return next(ctx, req)
			}
			if ctx == nil {
				var zero Res
				return zero, xerr.Validation("idempotency: context is required")
			}
			if err := ctx.Err(); err != nil {
				var zero Res
				return zero, err
			}
			if maxFlights < 0 {
				var zero Res
				return zero, xerr.Validation("idempotency: MaxConcurrentFlights must not be negative")
			}

			token, keyErr := extractIdempotencyKey(ctx, req, cfg)
			if keyErr != nil {
				var zero Res
				return zero, xerr.BadRequest("idempotency: could not derive request key", keyErr)
			}
			if token == "" {
				return next(ctx, req)
			}
			if err := validateIdempotencyKey(token); err != nil {
				var zero Res
				return zero, xerr.BadRequest("idempotency: invalid request key", err)
			}
			if cfg.Namespace == "" {
				var zero Res
				return zero, xerr.Validation("idempotency: a stable action namespace is required")
			}

			reqHash, hashErr := hashPayloadBytes(req)
			if hashErr != nil {
				var zero Res
				return zero, xerr.BadRequest("idempotency: request payload cannot be hashed", hashErr)
			}
			scopeHash := xctx.IdempotencyScopeHashFrom(ctx)
			key := scopedIdempotencyKey(cfg.Namespace, scopeHash, token)

			ttl := cfg.TTL
			if ttl <= 0 {
				ttl = DefaultIdempotencyTTL
			}

			if entry, ok := store.Get(ctx, key); ok {
				cached, err := replayIdempotencyEntry[Res](entry, reqHash)
				if err != nil {
					var zero Res
					return zero, err
				}
				return cached.value, nil
			}

			value, err := flights.Do(ctx, key, func(workCtx context.Context) (any, error) {
				workCtx, cancelFlight := withSharedFlightTimeout(workCtx, flightTimeout)
				if cancelFlight != nil {
					defer cancelFlight()
				}
				if err := workCtx.Err(); err != nil {
					return nil, err
				}
				if entry, ok := store.Get(workCtx, key); ok {
					return replayIdempotencyEntry[Res](entry, reqHash)
				}

				if coord, ok := store.(IdempotencyCoordinator); ok {
					claim, err := coord.Claim(workCtx, key, hashPayloadString(reqHash), cfg.EffectiveLeaseTTL())
					if err != nil {
						if ctxErr := workCtx.Err(); ctxErr != nil {
							return nil, ctxErr
						}
						return nil, xerr.Unavailable("idempotency coordination unavailable", err)
					}
					switch claim.State {
					case IdempotencyClaimCompleted:
						return replayIdempotencyEntry[Res](claim.Entry, reqHash)
					case IdempotencyClaimConflict:
						return nil, xerr.Conflict("idempotency key already used with a different request payload")
					case IdempotencyClaimInProgress:
						return nil, xerr.Conflict("an identical request is currently in progress")
					case IdempotencyClaimAcquired:
					default:
						return nil, xerr.Internal("idempotency: coordinator returned an unknown claim state")
					}

					var completed bool
					defer func() {
						if !completed {
							if releaseErr := coord.Release(context.WithoutCancel(workCtx), key, claim.Token); releaseErr != nil {
								slog.WarnContext(workCtx, "idempotency_claim_release_failed", "key", key, "error", releaseErr)
							}
						}
					}()

					stopRenewal := startLeaseRenewal(workCtx, coord, key, claim.Token, cfg.EffectiveLeaseTTL())
					renewalRunning := true
					defer func() {
						if renewalRunning {
							stopRenewal()
						}
					}()

					if err := workCtx.Err(); err != nil {
						return nil, err
					}
					res, execErr := next(workCtx, req)
					if execErr != nil {
						return nil, execErr
					}
					res = normalizeJSONNullResult(res)
					stopRenewal()
					renewalRunning = false

					entry, res, responseErr := idempotencyResponseEntry(workCtx, res, reqHash)
					if completeErr := coord.Complete(context.WithoutCancel(workCtx), key, claim.Token, entry, ttl); completeErr != nil {
						return nil, xerr.Unavailable("idempotency completion unavailable", completeErr)
					}
					completed = true
					if responseErr != nil {
						return nil, responseErr
					}
					return idempotentResult[Res]{requestHash: reqHash, value: res}, nil
				}

				if err := workCtx.Err(); err != nil {
					return nil, err
				}
				res, execErr := next(workCtx, req)
				if execErr != nil {
					return nil, execErr
				}
				res = normalizeJSONNullResult(res)
				entry, res, responseErr := idempotencyResponseEntry(workCtx, res, reqHash)
				store.Set(context.WithoutCancel(workCtx), key, entry, ttl)
				if responseErr != nil {
					return nil, responseErr
				}
				return idempotentResult[Res]{requestHash: reqHash, value: res}, nil
			})
			if err == nil && ctx.Err() != nil {
				var zero Res
				return zero, ctx.Err()
			}
			if err != nil {
				var zero Res
				return zero, err
			}
			result, ok := value.(idempotentResult[Res])
			if !ok {
				var zero Res
				return zero, xerr.Internal(fmt.Sprintf("idempotency result type mismatch: got %T", value))
			}
			if result.requestHash != reqHash {
				var zero Res
				return zero, xerr.Conflict("idempotency key already used with a different request payload")
			}
			return result.value, nil
		}
	}
}

// idempotencyResponseEntry always returns an entry so a successful handler
// execution is never forgotten merely because its response cannot be encoded.
// The invalid JSON marker makes subsequent requests fail closed rather than
// repeating the handler's side effects.
func idempotencyResponseEntry[Res any](ctx context.Context, res Res, requestHash [32]byte) (IdempotencyEntry, Res, error) {
	body, err := json.Marshal(res)
	if err == nil {
		res, err = normalizeJSONReplayResult(res, body)
	}
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
		return entry, res, xerr.Internal("idempotency: successful response cannot be serialized for replay", err)
	}
	return entry, res, nil
}

func extractIdempotencyKey(ctx context.Context, req any, cfg IdempotencyConfig) (string, error) {
	if key := xctx.IdempotencyKeyFrom(ctx); key != "" {
		return key, nil
	}
	if cfg.RequestKeyFunc != nil {
		return cfg.RequestKeyFunc(req)
	}
	if body, ok := req.([]byte); ok && cfg.KeyFunc != nil {
		return cfg.KeyFunc(body), nil
	}
	return "", nil
}

func validateIdempotencyKey(key string) error {
	if key == "" || len(key) > 255 {
		return errors.New("key must contain 1 to 255 printable ASCII bytes")
	}
	for index := range len(key) {
		if key[index] < 0x21 || key[index] > 0x7e {
			return errors.New("key must contain only printable ASCII bytes without whitespace")
		}
	}
	return nil
}

func scopedIdempotencyKey(namespace string, scopeHash [32]byte, token string) string {
	var storage [1024]byte
	data := storage[:0]
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len("kernel-idempotency-v1")))
	data = append(data, length[:]...)
	data = append(data, "kernel-idempotency-v1"...)
	binary.BigEndian.PutUint64(length[:], uint64(len(namespace)))
	data = append(data, length[:]...)
	data = append(data, namespace...)
	binary.BigEndian.PutUint64(length[:], uint64(len("scope-hash")))
	data = append(data, length[:]...)
	data = append(data, "scope-hash"...)
	data = append(data, scopeHash[:]...)
	binary.BigEndian.PutUint64(length[:], uint64(len(token)))
	data = append(data, length[:]...)
	data = append(data, token...)
	sum := sha256.Sum256(data)
	var encoded [len(idempotencyStorageKeyPrefix) + sha256.Size*2]byte
	copy(encoded[:], idempotencyStorageKeyPrefix)
	hex.Encode(encoded[len(idempotencyStorageKeyPrefix):], sum[:])
	return string(encoded[:])
}

func replayIdempotencyEntry[Res any](entry IdempotencyEntry, requestHash [32]byte) (idempotentResult[Res], error) {
	if requestHashMissing(entry) {
		return idempotentResult[Res]{}, xerr.Internal("idempotency: stored response is missing a valid request hash")
	}
	if requestHashConflict(entry, requestHash) {
		return idempotentResult[Res]{}, xerr.Conflict("idempotency key already used with a different request payload")
	}
	var cached Res
	if err := json.Unmarshal(entry.Body, &cached); err != nil {
		return idempotentResult[Res]{}, xerr.Internal("idempotency: stored response for key is corrupted")
	}
	return idempotentResult[Res]{requestHash: requestHash, value: cached}, nil
}

func hashPayloadString(sum [32]byte) string {
	if sum == ([32]byte{}) {
		return ""
	}
	return hex.EncodeToString(sum[:])
}

func requestHashConflict(entry IdempotencyEntry, requestHash [32]byte) bool {
	stored := entry.RequestHashBytes
	if stored == ([32]byte{}) {
		stored = decodeRequestHash(entry.RequestHash)
	}
	return stored != requestHash
}

func requestHashMissing(entry IdempotencyEntry) bool {
	if entry.RequestHashBytes != ([32]byte{}) {
		return false
	}
	return decodeRequestHash(entry.RequestHash) == ([32]byte{})
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
	done := make(chan struct{})
	renewCtx, cancelRenew := context.WithCancel(ctx)
	var stopOnce sync.Once
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				rCtx, cancel := context.WithTimeout(renewCtx, interval)
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
	return func() {
		stopOnce.Do(func() {
			close(stop)
			cancelRenew()
		})
		<-done
	}
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
