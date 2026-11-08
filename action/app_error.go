package action

import (
	"errors"

	"github.com/nexssp/kernel/xerr"
)

// appErrorFrom extracts an *xerr.AppError from err. errors.AsType may return
// (nil, true) when err wraps a typed-nil *xerr.AppError, so callers must check
// the pointer before dereferencing or propagating it.
func appErrorFrom(err error) (*xerr.AppError, bool) {
	return errors.AsType[*xerr.AppError](err)
}
