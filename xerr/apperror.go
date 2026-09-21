package xerr

import (
	"fmt"
	"runtime"
)

// RemoteErrorHeader marks a transport reply whose payload is an ErrorResponse.
const RemoteErrorHeader = "Nexss-Error"

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

// ErrorResponse is the public contract sent to clients — never expose internals.
type ErrorResponse struct {
	Error     string            `json:"error"`
	Message   string            `json:"message"`
	RequestID string            `json:"request_id,omitempty"`
	Details   ValidationDetails `json:"details,omitempty"`
}

// Public returns a safe, client-facing representation. Internal details never leak.
func (e *AppError) Public(requestID string) ErrorResponse {
	return ErrorResponse{
		Error:     string(e.Kind),
		Message:   e.Message,
		RequestID: requestID,
		Details:   e.ValidationDetails,
	}
}

// IsTransient reports whether the error may succeed on retry.
func (e *AppError) IsTransient() bool {
	return kindClassOf(e.Kind) == classTransient
}

// WithStack re-captures the stack. Use for bugs / unexpected internal errors.
func (e *AppError) WithStack() *AppError {
	e.Stack = captureStack()
	return e
}

// xerrPkgPrefix is the fully-qualified function-name prefix for this
// package's symbols in runtime.Frame.Function.
const xerrPkgPrefix = "github.com/nexssp/kernel/xerr."

// captureStack records the stack.
//
// We skip runtime.Callers, captureStack, and WithStack using Callers(3, ...).
// The resulting raw PCs are stored unmodified in the stack slice.
// We must NEVER unpack the PCs and store `frame.PC` back into a slice, as
// passing a resolved `frame.PC` back into runtime.CallersFrames causes it to
// erroneously adjust the instruction pointer a second time, corrupting the
// line numbers and breaking inline frame expansion.
//
// Filtering of xerrPkgPrefix frames is handled natively at print/format time
// by `isUserFrame`.
func captureStack() []uintptr {
	var pcs [64]uintptr
	n := runtime.Callers(3, pcs[:])
	if n == 0 {
		return nil
	}

	out := make([]uintptr, n)
	copy(out, pcs[:n])
	return out
}
