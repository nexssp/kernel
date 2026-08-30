package xerr

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
)

func IsDev() bool { return isDev }

// RemoteErrorHeader marks a transport reply whose payload is an ErrorResponse.
// It is intentionally transport-neutral so all adapters share one safe-error contract.
const RemoteErrorHeader = "Nexss-Error"

// ErrorResponse is the public contract sent to clients — never expose internals.
type ErrorResponse struct {
	Error     string            `json:"error"`
	Message   string            `json:"message"`
	RequestID string            `json:"request_id,omitempty"`
	Details   ValidationDetails `json:"details,omitempty"`
}

// AppError is the single error type for all application errors.
type AppError struct {
	Kind              Kind
	Message           string
	Cause             error
	Stack             []uintptr
	ValidationDetails ValidationDetails
}

func (e *AppError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Kind, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Kind, e.Message)
}

func (e *AppError) Unwrap() error { return e.Cause }

// Public returns a safe, client-facing representation. Internal details never leak.
func (e *AppError) Public(reqID string) ErrorResponse {
	return ErrorResponse{
		Error:     string(e.Kind),
		Message:   e.Message,
		RequestID: reqID,
		Details:   e.ValidationDetails,
	}
}

// FromPublic reconstructs a safe AppError from an ErrorResponse received over a
// trusted transport boundary. It never restores a remote cause or stack trace.
// The bool is false when the response does not contain a recognized Nexss kind.
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

func isKnownKind(kind Kind) bool {
	switch kind {
	case KindBadRequest, KindUnauthorized, KindForbidden, KindNotFound,
		KindConflict, KindValidation, KindTooManyRequests, KindTimeout,
		KindUnavailable, KindInternal, KindMethodNotAllowed, KindRateLimit,
		KindCanceled, KindDatabase, KindShutdown, KindCircuitBreaker:
		return true
	default:
		return false
	}
}

// IsTransient reports whether the error may succeed on retry.
// Used by RetryMiddleware and circuit breakers.
func (e *AppError) IsTransient() bool {
	switch e.Kind {
	case KindTimeout, KindUnavailable, KindCircuitBreaker, KindRateLimit, KindTooManyRequests:
		return true
	}
	return false
}

// WithStack re-captures the stack. Use for bugs / unexpected internal errors.
func (e *AppError) WithStack() *AppError {
	e.Stack = captureStack()
	return e
}

func captureStack() []uintptr {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	return pcs[0:n]
}

// ── Constructors ──────────────────────────────────────────────────────────────

func BadRequest(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindBadRequest, Message: msg, Cause: first(cause)}
}

func Unauthorized(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindUnauthorized, Message: msg, Cause: first(cause)}
}

func ServiceUnavailable(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "service unavailable"
	}
	return &AppError{Kind: KindUnavailable, Message: msg, Cause: first(cause)}
}

func Forbidden(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindForbidden, Message: msg, Cause: first(cause)}
}

func NotFound(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindNotFound, Message: msg, Cause: first(cause)}
}

func Conflict(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindConflict, Message: msg, Cause: first(cause)}
}

func MethodNotAllowed(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "method not allowed"
	}
	return &AppError{Kind: KindMethodNotAllowed, Message: msg, Cause: first(cause)}
}

func TooManyRequests(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "too many requests"
	}
	return &AppError{Kind: KindTooManyRequests, Message: msg, Cause: first(cause)}
}

func Timeout(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "request timeout"
	}
	return &AppError{Kind: KindTimeout, Message: msg, Cause: first(cause)}
}

func Unavailable(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "service unavailable"
	}
	return &AppError{Kind: KindUnavailable, Message: msg, Cause: first(cause)}
}

func Canceled(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "request canceled"
	}
	return &AppError{Kind: KindCanceled, Message: msg, Cause: first(cause)}
}

func RateLimit(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "rate limit exceeded"
	}
	return &AppError{Kind: KindRateLimit, Message: msg, Cause: first(cause)}
}

func CircuitBreaker(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "circuit breaker open"
	}
	return &AppError{Kind: KindCircuitBreaker, Message: msg, Cause: first(cause)}
}

func Database(msg string, cause ...error) *AppError {
	return (&AppError{Kind: KindDatabase, Message: msg, Cause: first(cause)}).WithStack()
}

func Shutdown(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindShutdown, Message: msg, Cause: first(cause)}
}

// Internal captures a stack trace — use only for unexpected bugs.
func Internal(msg string, cause ...error) *AppError {
	if msg == "" {
		msg = "internal server error"
	}
	return (&AppError{Kind: KindInternal, Message: msg, Cause: first(cause)}).WithStack()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func first(errs []error) error {
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// From converts any error into an AppError. Never returns nil if err != nil.
func From(err error) *AppError {
	if err == nil {
		return nil
	}
	var e *AppError
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Timeout("operation timed out", err)
	}
	if errors.Is(err, context.Canceled) {
		return Canceled("operation canceled", err)
	}
	return Internal("internal server error", err)
}

// IsTransient is the package-level predicate used by retry and circuit-breaker logic.
func IsTransient(err error) bool {
	var transient interface{ IsTransient() bool }
	if errors.As(err, &transient) {
		return transient.IsTransient()
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
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
