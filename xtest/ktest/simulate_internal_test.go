package ktest

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest"
)

func TestSimulate_RejectsZeroConcurrency(t *testing.T) {
	xtest.ExpectFatal(t, "TestSimulate_RejectsZeroConcurrency", "XTEST_SIMULATE_ZERO",
		"concurrency must be > 0",
		func(t *testing.T) {
			act := action.New("x", func(_ context.Context, _ struct{}) (struct{}, error) {
				return struct{}{}, nil
			}).Build()
			Simulate(t, act, struct{}{}, 0, func(testing.TB, struct{}, error) {})
		})
}
