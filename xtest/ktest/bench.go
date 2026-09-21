package ktest

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

// BenchAction benchmarks a built action's pure execution speed without
// transport overhead. Use it to lock in the zero-allocation hot path on
// Do/DoAny: a regression shows up in allocs/op immediately.
func BenchAction[Req, Res any](b *testing.B, act *action.BuiltAction[Req, Res], req Req) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := act.Do(ctx, req); err != nil {
				b.Errorf("unexpected error during benchmark: %v", err)
			}
		}
	})
}
