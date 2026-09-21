package ktest_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest/ktest"
)

func BenchmarkBenchAction_Smoke(b *testing.B) {
	act := action.New("fast.action", func(_ context.Context, n int) (int, error) {
		return n * 2, nil
	}).Build()
	ktest.BenchAction(b, act, 42)
}
