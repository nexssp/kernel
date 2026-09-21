package ktest_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestRun_NoErrorAndEquals(t *testing.T) {
	act := ktest.NewOkAction("test").Build()

	ktest.Run(t, act, context.Background(), struct{}{}).
		NoError().
		Equals("ok").
		Contains("k")
}

func TestRun_ErrorKind(t *testing.T) {
	act := ktest.NewErrAction("test", xerr.Forbidden("stop")).Build()

	ktest.Run(t, act, context.Background(), struct{}{}).
		ErrorKind(xerr.KindForbidden)
}
