package ktest_test

import (
	"errors"
	"testing"

	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestRequireErrorKind_Pass(t *testing.T) {
	err := xerr.NotFound("user missing")
	ktest.RequireErrorKind(t, err, xerr.KindNotFound)
}

func TestRequireErrorKind_WrongKind(t *testing.T) {
	xtest.ExpectFatal(t, "TestRequireErrorKind_WrongKind", "XTEST_REQ_KIND_WRONG",
		`expected kind "NotFound"`,
		func(t *testing.T) {
			err := xerr.BadRequest("bad")
			ktest.RequireErrorKind(t, err, xerr.KindNotFound)
		})
}

func TestRequireErrorKind_NilError(t *testing.T) {
	xtest.ExpectFatal(t, "TestRequireErrorKind_NilError", "XTEST_REQ_KIND_NIL",
		"got nil error",
		func(t *testing.T) {
			ktest.RequireErrorKind(t, nil, xerr.KindNotFound)
		})
}

func TestRequireTransient(t *testing.T) {
	ktest.RequireTransient(t, xerr.Unavailable("down"))
	ktest.RequireTransient(t, xerr.Timeout("slow"))
	ktest.RequireTransient(t, xerr.CircuitBreaker("open"))
}

func TestRequireTransient_OnPermanent(t *testing.T) {
	xtest.ExpectFatal(t, "TestRequireTransient_OnPermanent", "XTEST_REQ_TRANSIENT",
		"expected transient",
		func(t *testing.T) {
			ktest.RequireTransient(t, xerr.NotFound("missing"))
		})
}

func TestRequirePermanent(t *testing.T) {
	ktest.RequirePermanent(t, xerr.BadRequest("bad"))
	ktest.RequirePermanent(t, xerr.Validation("invalid"))
}

func TestRequirePermanent_OnTransient(t *testing.T) {
	xtest.ExpectFatal(t, "TestRequirePermanent_OnTransient", "XTEST_REQ_PERMANENT",
		"expected permanent",
		func(t *testing.T) {
			ktest.RequirePermanent(t, xerr.Unavailable("down"))
		})
}

func TestRequireTransient_OnPlainError(t *testing.T) {
	xtest.ExpectFatal(t, "TestRequireTransient_OnPlainError", "XTEST_REQ_PLAIN",
		"expected transient",
		func(t *testing.T) {
			ktest.RequireTransient(t, errors.New("raw"))
		})
}
