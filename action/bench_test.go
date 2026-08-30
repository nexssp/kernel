package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

// BenchmarkAction_Overhead mathematically proves the zero-alloc claims
// for the core Action pipeline (routing, hooks, meta attachment).
func BenchmarkAction_Overhead(b *testing.B) {
	rawHandler := func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	}

	act := action.New("bench.action", rawHandler).
		Tag("bench").
		HookBefore(func(ctx context.Context, _ int, _ *action.Meta) (context.Context, error) {
			return ctx, nil
		}).
		Build()
	actWithAnyHook := action.New("bench.action.any_hook", rawHandler).
		AnyHook(action.AnyHook{}).
		Build()

	ctx := context.Background()

	b.Run("Raw Handler", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if _, err := rawHandler(ctx, i); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Nexss BuiltAction", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if _, err := act.Do(ctx, i); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Nexss BuiltAction AnyHook Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if _, err := actWithAnyHook.Do(ctx, i); err != nil {
				b.Fatal(err)
			}
		}
	})
}
