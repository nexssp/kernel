// Benchmarks guiding the decision between typed (generic) and any-erased
// composition. Numbers here decide whether flow stays on AnyAction at
// the DSL boundary, or whether we need a typed fast path.
package action_test

import (
	"context"
	"iter"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
)

type User struct {
	ID   string
	Name string
	Age  int
}

func makeUserAction() *action.Builder[User, User] {
	return action.New("step", func(_ context.Context, u User) (User, error) { return u, nil })
}

func BenchmarkSingle_Typed(b *testing.B) {
	act := makeUserAction().Build()
	ctx := context.Background()
	u := User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := act.Do(ctx, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingle_DoAny_Preboxed(b *testing.B) {
	act := makeUserAction().Build()
	ctx := context.Background()
	var in any = User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := act.DoAny(ctx, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingle_DoAny_BoxAtCall(b *testing.B) {
	act := makeUserAction().Build()
	ctx := context.Background()
	u := User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := act.DoAny(ctx, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingle_InvokeAny_Preboxed(b *testing.B) {
	act := makeUserAction().Build()
	ctx := context.Background()
	var in any = User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := action.InvokeAny(ctx, act, in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSingle_InvokeAny_MapInput(b *testing.B) {
	act := makeUserAction().Build()
	ctx := context.Background()
	m := map[string]any{"ID": "1", "Name": "alice", "Age": 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := action.InvokeAny(ctx, act, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChain_Typed_10(b *testing.B) {
	builders := make([]*action.Builder[User, User], 10)
	for i := range builders {
		builders[i] = makeUserAction()
	}
	chain := action.Chain("chain", builders...).Build()
	ctx := context.Background()
	u := User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := chain.Do(ctx, u); err != nil {
			b.Fatal(err)
		}
	}
}

func pipeAnyBench(left, right action.AnyAction) action.AnyAction {
	return action.New("pipe", func(ctx context.Context, req any) (any, error) {
		mid, err := action.InvokeAny(ctx, left, req)
		if err != nil {
			return nil, err
		}
		return action.InvokeAny(ctx, right, mid)
	}).Build()
}

func BenchmarkChain_Any_10(b *testing.B) {
	acts := make([]action.AnyAction, 10)
	for i := range acts {
		acts[i] = makeUserAction().Build()
	}
	chain := acts[0]
	for i := 1; i < len(acts); i++ {
		chain = pipeAnyBench(chain, acts[i])
	}

	ctx := context.Background()
	var in any = User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := action.InvokeAny(ctx, chain, in); err != nil {
			b.Fatal(err)
		}
	}
}

func makeIntStream() *action.StreamAction[struct{}, int] {
	return action.NewStream("src", func(_ context.Context, _ struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) {
			for i := range 1000 {
				if !yield(i, nil) {
					return
				}
			}
		}, nil
	})
}

func BenchmarkStream_Typed_1000(b *testing.B) {
	src := makeIntStream()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		seq, err := src.Do(ctx, struct{}{})
		if err != nil {
			b.Fatal(err)
		}
		for item, err := range seq {
			if err != nil {
				b.Fatal(err)
			}
			_ = item
		}
	}
}

func BenchmarkStream_AnyWrapped_1000(b *testing.B) {
	src := makeIntStream()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		s, err := src.DoStreamAny(ctx, struct{}{})
		if err != nil {
			b.Fatal(err)
		}
		s(func(item any, err error) bool {
			if err != nil {
				b.Fatal(err)
			}
			if _, ok := item.(int); !ok {
				b.Fatalf("expected int, got %T", item)
			}
			return true
		})
	}
}

func BenchmarkPool_Typed_RoundRobin(b *testing.B) {
	members := make([]*action.BuiltAction[User, User], 10)
	for i := range members {
		members[i] = makeUserAction().Build()
	}

	var counter atomic.Uint64
	act := action.New("pool", func(ctx context.Context, u User) (User, error) {
		slot := counter.Add(1) - 1
		return members[slot%uint64(len(members))].Do(ctx, u)
	}).Build()

	ctx := context.Background()
	u := User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := act.Do(ctx, u); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPool_Any_RoundRobin(b *testing.B) {
	members := make([]action.AnyAction, 10)
	for i := range members {
		members[i] = makeUserAction().Build()
	}

	var counter atomic.Uint64
	act := action.New("pool", func(ctx context.Context, req any) (any, error) {
		slot := counter.Add(1) - 1
		return action.InvokeAny(ctx, members[slot%uint64(len(members))], req)
	}).Build()

	ctx := context.Background()
	var in any = User{ID: "1", Name: "alice", Age: 30}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := action.InvokeAny(ctx, act, in); err != nil {
			b.Fatal(err)
		}
	}
}
