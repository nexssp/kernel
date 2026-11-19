// Package secret provides opaque secret values.
package secret

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// Redacted is the representation used by formatting, logging, and JSON/text serialization.
const Redacted = "[REDACTED]"

var (
	// ErrNilCallback indicates that WithValue was called without a callback.
	ErrNilCallback = errors.New("secret: value callback is nil")
	// ErrCannotSerialize indicates that secret values cannot be encoded with gob.
	ErrCannotSerialize = errors.New("secret: gob encoding of secret values is not supported")
	// ErrCannotDeserialize indicates that serialized secret values cannot be restored.
	ErrCannotDeserialize = errors.New("secret: serialized values cannot be restored; supply the secret again")
)

// Value is an opaque secret. Its zero value is empty. Use New to construct a
// value and WithValue for the narrow operation that must consume its contents.
// Value deliberately has no plaintext String or Unwrap method and is not
// comparable. reflect.DeepEqual can still inspect its private content and is
// not constant-time; use Equal for secret comparisons.
type Value struct {
	_     [0]func()
	value string
}

// New constructs an opaque secret value. The input string is retained as the
// private representation; callers should avoid keeping unnecessary copies.
func New(value string) Value { return Value{value: value} }

// IsEmpty reports whether value contains no secret bytes.
func IsEmpty(value Value) bool { return value.value == "" }

// Equal reports whether a and b contain the same secret bytes. For equal-length
// values, the comparison is constant-time. Different lengths return
// immediately, so secret length is observable. Do not use reflect.DeepEqual to
// compare secrets; it can inspect Value's private content and is not constant-time.
func Equal(a, b Value) bool {
	aBytes := []byte(a.value)
	defer clear(aBytes)
	bBytes := []byte(b.value)
	defer clear(bBytes)
	return subtle.ConstantTimeCompare(aBytes, bBytes) == 1
}

// WithValue provides a temporary byte copy of value to fn and clears that copy
// when fn returns. Callers must not retain or copy the bytes. Go cannot promise
// erasure of the immutable source string or any copies made by the caller.
func WithValue(value Value, fn func([]byte) error) error {
	if fn == nil {
		return ErrNilCallback
	}
	bytes := []byte(value.value)
	defer clear(bytes)
	return fn(bytes)
}

// Format prevents fmt from exposing the secret for any formatting verb.
func (Value) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", Redacted)
		return
	}
	if _, err := io.WriteString(state, Redacted); err != nil {
		return
	}
}

// MarshalJSON always emits a redaction marker, never the secret contents.
func (Value) MarshalJSON() ([]byte, error) { return json.Marshal(Redacted) }

// MarshalText always emits a redaction marker, never the secret contents.
func (Value) MarshalText() ([]byte, error) { return []byte(Redacted), nil }

// GobEncode refuses to serialize secret values through gob.
func (Value) GobEncode() ([]byte, error) { return nil, ErrCannotSerialize }

// LogValue prevents slog from recording the secret contents.
func (Value) LogValue() slog.Value { return slog.StringValue(Redacted) }

// UnmarshalJSON refuses to restore a secret from serialized data.
func (value *Value) UnmarshalJSON([]byte) error {
	if value == nil {
		return errors.New("secret: cannot unmarshal into nil value")
	}
	return ErrCannotDeserialize
}

// UnmarshalText refuses to restore a secret from serialized data.
func (value *Value) UnmarshalText([]byte) error {
	if value == nil {
		return errors.New("secret: cannot unmarshal into nil value")
	}
	return ErrCannotDeserialize
}

// GobDecode refuses to restore a secret value from gob data.
func (*Value) GobDecode([]byte) error { return ErrCannotDeserialize }
