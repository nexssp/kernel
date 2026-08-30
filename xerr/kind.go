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

func AllKinds() []Kind {
	return []Kind{
		KindBadRequest, KindUnauthorized, KindForbidden, KindNotFound,
		KindConflict, KindValidation, KindTooManyRequests, KindTimeout,
		KindUnavailable, KindInternal, KindMethodNotAllowed, KindRateLimit,
		KindCanceled, KindDatabase, KindShutdown, KindCircuitBreaker,
	}
}

// KindFrom extracts the Kind from an error.
// Defaults to KindInternal if the error is not an AppError.
func KindFrom(err error) Kind {
	if err == nil {
		return ""
	}
	var ae *AppError
	if errors.As(err, &ae) {
		return ae.Kind
	}
	return KindInternal
}
