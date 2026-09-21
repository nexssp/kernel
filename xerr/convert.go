package xerr

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// From converts any error into an AppError. Never returns nil if err != nil.
// Recognized context and network errors are classified by classify; the
// remainder is wrapped as Internal because an unrecognized error at a
// general boundary is a programmer error, not a transient condition.
func From(err error) *AppError {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*AppError](err); ok {
		return e
	}
	if ae := classify(err); ae != nil {
		return ae
	}
	return Internal("internal server error", err)
}

// PanicRecovery wraps a recovered panic value as an Internal error with stack trace.
func PanicRecovery(recovered any) *AppError {
	switch v := recovered.(type) {
	case error:
		return Internal("panic recovered", v)
	case string:
		return Internal("panic recovered", errors.New(v))
	default:
		return Internal("panic recovered", fmt.Errorf("%v", v))
	}
}

// IsPermanent reports whether the error is fundamentally non-retryable
// (e.g. 4xx client errors like validation or unauthorized). Backed by
// kindClassOf so it cannot drift from IsTransient.
func IsPermanent(err error) bool {
	k := KindFrom(err)
	if k == "" {
		return false
	}
	return kindClassOf(k) == classPermanent
}

type transienceCheck interface {
	error
	IsTransient() bool
}

// IsTransient is the package-level predicate used by retry and
// circuit-breaker logic. It honors both a self-classifying error (one
// that implements IsTransient() bool) and a net.Error whose Timeout()
// returns true.
func IsTransient(err error) bool {
	if t, ok := errors.AsType[transienceCheck](err); ok {
		return t.IsTransient()
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return true
	}
	return false
}

// classify maps recognized network and context errors to an *AppError,
// or returns nil when err does not match any known pattern. Single shared
// classifier used by both From and MapTransportError.
func classify(err error) *AppError {
	if errors.Is(err, context.DeadlineExceeded) {
		return Timeout("operation timed out", err)
	}
	if errors.Is(err, context.Canceled) {
		return Canceled("operation canceled", err)
	}
	if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
		return Unavailable("DNS resolution failed", dnsErr)
	}
	if netErr, ok := errors.AsType[net.Error](err); ok {
		if netErr.Timeout() {
			return Timeout("network timeout", err)
		}
		return Unavailable("network error", err)
	}
	return nil
}

// MapTransportError converts raw transport or network errors into the
// xerr taxonomy. Call at every adapter boundary (HTTP, NATS, gRPC).
// Unrecognized errors are wrapped as Unavailable, because at a transport
// boundary an unknown error is a connectivity problem, not a bug.
func MapTransportError(err error) error {
	if err == nil {
		return nil
	}
	if ae := classify(err); ae != nil {
		return ae
	}
	return Unavailable("transport error", err)
}

// FromPublic reconstructs a safe AppError from an ErrorResponse received
// over a trusted transport boundary. It never restores a remote cause or
// stack trace. The bool is false when the response does not contain a
// recognized Nexss kind.
func FromPublic(response ErrorResponse) (*AppError, bool) {
	kind := Kind(response.Error)
	if !isKnownKind(kind) {
		return nil, false
	}

	message := response.Message
	if message == "" {
		message = "remote request failed"
	}
	return &AppError{
		Kind:              kind,
		Message:           message,
		ValidationDetails: response.Details,
	}, true
}

var knownKinds = func() map[Kind]struct{} {
	m := make(map[Kind]struct{}, len(AllKinds()))
	for _, k := range AllKinds() {
		m[k] = struct{}{}
	}
	return m
}()

func isKnownKind(kind Kind) bool {
	_, ok := knownKinds[kind]
	return ok
}
