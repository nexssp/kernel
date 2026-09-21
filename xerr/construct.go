package xerr

func BadRequest(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindBadRequest, Message: msg, Cause: first(cause)}
}

func Unauthorized(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindUnauthorized, Message: msg, Cause: first(cause)}
}

// Deprecated: use Unavailable instead.
func ServiceUnavailable(msg string, cause ...error) *AppError {
	return Unavailable(msg, cause...)
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

func first(errs []error) error {
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
