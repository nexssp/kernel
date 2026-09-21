package ktest_test

import (
	"testing"

	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestPredefined_Echo(t *testing.T) {
	act := ktest.Echo[string]("echo").Build()
	ktest.Run(t, act, t.Context(), "payload").NoError().Equals("payload")
}

func TestPredefined_Returns(t *testing.T) {
	act := ktest.Returns[string, int]("constant", 42).Build()
	ktest.Run(t, act, t.Context(), "ignored").NoError().Equals(42)
}

func TestPredefined_Fails(t *testing.T) {
	act := ktest.Fails[string, int]("failure", xerr.Unavailable("down")).Build()
	ktest.Run(t, act, t.Context(), "request").ErrorKind(xerr.KindUnavailable)
}

func TestPredefined_SequenceRepeatsLastValue(t *testing.T) {
	act := ktest.Sequence[string, string]("sequence", "tx-1", "tx-2").Build()
	ktest.Run(t, act, t.Context(), "request").NoError().Equals("tx-1")
	ktest.Run(t, act, t.Context(), "request").NoError().Equals("tx-2")
	ktest.Run(t, act, t.Context(), "request").NoError().Equals("tx-2")
}

func TestPredefined_FlakyRecoversAfterConfiguredFailures(t *testing.T) {
	act := ktest.Flaky[string, string](
		"flaky",
		2,
		xerr.Unavailable("temporary"),
		"ok",
	).Build()

	ktest.Run(t, act, t.Context(), "request").ErrorKind(xerr.KindUnavailable)
	ktest.Run(t, act, t.Context(), "request").ErrorKind(xerr.KindUnavailable)
	ktest.Run(t, act, t.Context(), "request").NoError().Equals("ok")
}

func TestPredefined_FactoriesReturnBuilders(_ *testing.T) {
	_ = ktest.Echo[string]("echo")
	_ = ktest.Returns[string, string]("returns", "ok")
	_ = ktest.Fails[string, string]("fails", nil)
	_ = ktest.Sequence[string, string]("sequence", "ok")
	_ = ktest.Flaky[string, string]("flaky", 0, nil, "ok")
}
