package xerr

import "fmt"

// ValidationDetail describes a single field validation failure.
type ValidationDetail struct {
	Field      string `json:"field"`
	Validation string `json:"validation"`
	Value      string `json:"value,omitempty"`
}

type ValidationDetails []ValidationDetail

func (v ValidationDetail) String() string {
	return fmt.Sprintf("[%s: %s %s]", v.Field, v.Validation, v.Value)
}

// Validation creates a typed validation error with structured details.
func Validation(msg string, cause ...error) *AppError {
	return &AppError{Kind: KindValidation, Message: msg, Cause: first(cause)}
}
