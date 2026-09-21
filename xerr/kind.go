package xerr

import "errors"

// Kind is the machine-readable error category.
type Kind string

const (
	KindBadRequest      Kind = "BadRequest"
	KindUnauthorized    Kind = "Unauthorized"
	KindForbidden       Kind = "Forbidden"
	KindNotFound        Kind = "NotFound"
	KindConflict        Kind = "Conflict"
	KindValidation      Kind = "Validation"
	KindTooManyRequests Kind = "TooManyRequests"
	KindTimeout         Kind = "Timeout"
	KindUnavailable     Kind = "Unavailable"
	KindInternal        Kind = "Internal"

	// Extended kinds
	KindMethodNotAllowed Kind = "MethodNotAllowed"
	KindRateLimit        Kind = "RateLimit"
	KindCanceled         Kind = "Canceled"
	KindDatabase         Kind = "Database"
	KindShutdown         Kind = "Shutdown"
	KindCircuitBreaker   Kind = "CircuitBreaker"
)

// allKinds is the canonical list. AllKinds returns a view into it, so
// callers must not mutate the returned slice.
var allKinds = [...]Kind{
	KindBadRequest, KindUnauthorized, KindForbidden, KindNotFound,
	KindConflict, KindValidation, KindTooManyRequests, KindTimeout,
	KindUnavailable, KindInternal, KindMethodNotAllowed, KindRateLimit,
	KindCanceled, KindDatabase, KindShutdown, KindCircuitBreaker,
}

func AllKinds() [16]Kind { return allKinds }

// KindFrom extracts the Kind from an error. Defaults to KindInternal
// when the error is not an AppError.
func KindFrom(err error) Kind {
	if err == nil {
		return ""
	}
	if ae, ok := errors.AsType[*AppError](err); ok {
		return ae.Kind
	}
	return KindInternal
}

// kindClass is a three-way classification used by both IsTransient and
// IsPermanent. It has three values on purpose: KindInternal, KindDatabase,
// KindCanceled, KindConflict, KindShutdown, and KindMethodNotAllowed are
// deliberately neither transient (safe to retry) nor permanent (must not
// be retried). Collapsing these to a boolean would silently reclassify
// them, changing retry and circuit-breaker behavior.
type kindClass uint8

const (
	classUnclassified kindClass = iota // neither retryable nor permanent
	classTransient                     // safe to retry
	classPermanent                     // must not be retried
)

// kindClassOf returns the classification of k. The single source of
// truth: IsTransient and IsPermanent both read it, so they cannot drift.
func kindClassOf(k Kind) kindClass {
	switch k {
	case KindTimeout, KindUnavailable, KindCircuitBreaker,
		KindRateLimit, KindTooManyRequests:
		return classTransient

	case KindBadRequest, KindUnauthorized, KindForbidden,
		KindNotFound, KindValidation:
		return classPermanent

	case KindConflict, KindInternal, KindMethodNotAllowed,
		KindCanceled, KindDatabase, KindShutdown:
		return classUnclassified

	default:
		return classUnclassified
	}
}
