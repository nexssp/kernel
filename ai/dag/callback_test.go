package dag

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
)

func echo(name string) action.AnyAction {
	return action.New(name, func(_ context.Context, _ struct{}) (string, error) {
		return name, nil
	}).Build()
}

func mustGraph(t *testing.T, build func(b *Builder) *Builder) *DAG {
	t.Helper()
	g, err := build(New(t.Name())).Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestLayerCallback_FiresOncePerLayer(t *testing.T) {
	g := mustGraph(t, func(b *Builder) *Builder {
		return b.
			AddNode("a", "a_out", echo("a")).
			AddNode("b", "b_out", echo("b")).
			AddNode("c", "c_out", echo("c")).
			AddEdge("a", "c").
			AddEdge("b", "c")
	})

	var layers []int
	ctx := WithLayerCallback(context.Background(),
		func(_ context.Context, layer int, _ *State) error {
			layers = append(layers, layer)
			return nil
		})

	state := AcquireState()
	defer state.Release()
	final, err := g.Execute(ctx, state.AsRead())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer final.Release()

	if len(layers) != 2 {
		t.Fatalf("callback fired %d times, want 2 (layers=%v)", len(layers), layers)
	}
	if layers[0] != 0 || layers[1] != 1 {
		t.Fatalf("layers = %v, want [0 1]", layers)
	}
}

func TestLayerCallback_ReceivesAccumulatedState(t *testing.T) {
	g := mustGraph(t, func(b *Builder) *Builder {
		return b.
			AddNode("a", "a_out", echo("a")).
			AddNode("b", "b_out", echo("b")).
			AddEdge("a", "b")
	})

	var (
		sawAAtLayer0     bool
		sawAAndBAtLayer1 bool
	)

	ctx := WithLayerCallback(context.Background(),
		func(_ context.Context, layer int, state *State) error {
			aVal, hasA := state.Get(OutputKey("a"))
			bVal, hasB := state.Get(OutputKey("b"))
			switch layer {
			case 0:
				if hasA && aVal == "a" && !hasB {
					sawAAtLayer0 = true
				}
			case 1:
				if hasA && aVal == "a" && hasB && bVal == "b" {
					sawAAndBAtLayer1 = true
				}
			}
			return nil
		})

	state := AcquireState()
	defer state.Release()
	final, err := g.Execute(ctx, state.AsRead())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer final.Release()

	if !sawAAtLayer0 {
		t.Error("layer 0 callback did not see a's output only")
	}
	if !sawAAndBAtLayer1 {
		t.Error("layer 1 callback did not see both a and b outputs")
	}
}

func TestLayerCallback_ErrorAbortsAndPreservesState(t *testing.T) {
	g := mustGraph(t, func(b *Builder) *Builder {
		return b.
			AddNode("a", "a_out", echo("a")).
			AddNode("b", "b_out", echo("b")).
			AddEdge("a", "b")
	})

	sentinel := errors.New("stop after layer 0")
	var calls int

	ctx := WithLayerCallback(context.Background(),
		func(_ context.Context, layer int, _ *State) error {
			calls++
			if layer == 0 {
				return sentinel
			}
			return nil
		})

	state := AcquireState()
	final, err := g.Execute(ctx, state.AsRead())
	defer state.Release()

	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (layer 1 must not run after callback error)", calls)
	}
	if final == nil {
		t.Fatal("state must be preserved on callback error, got nil")
	}
	defer final.Release()

	if v, ok := final.Get(OutputKey("a")); !ok || v != "a" {
		t.Fatalf("preserved state missing a's output: v=%v ok=%v", v, ok)
	}
	if _, ok := final.Get(OutputKey("b")); ok {
		t.Fatal("b ran despite callback abort")
	}
}

func TestLayerCallback_NilCallbackSafe(t *testing.T) {
	g := mustGraph(t, func(b *Builder) *Builder {
		return b.AddNode("a", "a_out", echo("a"))
	})

	state := AcquireState()
	defer state.Release()
	final, err := g.Execute(context.Background(), state.AsRead())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer final.Release()

	if v, _ := final.Get(OutputKey("a")); v != "a" {
		t.Fatalf("a output = %v, want a", v)
	}
}

func TestLayerCallback_ConsumedByOutermostOnly(t *testing.T) {
	inner := mustGraph(t, func(b *Builder) *Builder {
		return b.AddNode("x", "x_out", echo("x"))
	})

	outer := mustGraph(t, func(b *Builder) *Builder {
		return b.
			AddNode("inner", "inner_out", inner.AsAction().Build()).
			AddNode("after", "after_out", echo("after")).
			AddEdge("inner", "after")
	})

	var layers []int
	ctx := WithLayerCallback(context.Background(),
		func(_ context.Context, layer int, _ *State) error {
			layers = append(layers, layer)
			return nil
		})

	state := AcquireState()
	defer state.Release()
	final, err := outer.Execute(ctx, state.AsRead())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer final.Release()

	if len(layers) != 2 {
		t.Fatalf("callback fired %d times, want 2 (outer only), layers=%v", len(layers), layers)
	}
	if layers[0] != 0 || layers[1] != 1 {
		t.Fatalf("layers = %v, want [0 1]", layers)
	}
}

func TestSuspend_MatchesErrSuspendedAndCarriesPayload(t *testing.T) {
	err := Suspend("awaiting approval", map[string]any{"amount": 12000})

	if !errors.Is(err, ErrSuspended) {
		t.Fatal("errors.Is(err, ErrSuspended) must be true")
	}

	var se *SuspendError
	if !errors.As(err, &se) {
		t.Fatal("errors.As must resolve to *SuspendError")
	}
	if se.Reason != "awaiting approval" {
		t.Fatalf("Reason = %q", se.Reason)
	}
	payload, ok := se.Payload.(map[string]any)
	if !ok || payload["amount"] != 12000 {
		t.Fatalf("Payload lost: %+v", se.Payload)
	}
}

func TestLayerCallback_PrePopulatedStateSkipsNode(t *testing.T) {
	g := mustGraph(t, func(b *Builder) *Builder {
		return b.
			AddNode("a", "a_out", echo("a")).
			AddNode("b", "b_out", echo("b")).
			AddEdge("a", "b")
	})

	state := AcquireState()
	state.Set(OutputKey("a"), "preset-a")

	var bRan bool
	ctx := WithLayerCallback(context.Background(),
		func(_ context.Context, _ int, s *State) error {
			if v, ok := s.Get(OutputKey("b")); ok && v == "b" {
				bRan = true
			}
			return nil
		})

	final, err := g.Execute(ctx, state.AsRead())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer final.Release()
	defer state.Release()

	if !bRan {
		t.Fatal("b should have run in the resumed execution")
	}
	if v, _ := final.Get(OutputKey("a")); v != "preset-a" {
		t.Fatalf("a's preset output was overwritten: %v", v)
	}
}

func TestLayerCallback_SuspendPreservesSentinel(t *testing.T) {
	g := mustGraph(t, func(b *Builder) *Builder {
		return b.AddNode("a", "a_out", echo("a"))
	})

	ctx := WithLayerCallback(context.Background(),
		func(_ context.Context, _ int, _ *State) error {
			return Suspend("awaiting approval", map[string]any{"amount": 12000})
		})

	state := AcquireState()
	defer state.Release()

	final, err := g.Execute(ctx, state.AsRead())

	if !errors.Is(err, ErrSuspended) {
		t.Fatalf("expected ErrSuspended, got %v", err)
	}
	if _, ok := errors.AsType[*ExecutionError](err); ok {
		t.Fatal("ErrSuspended must not be wrapped in *ExecutionError")
	}
	if final == nil {
		t.Fatal("state must be preserved on suspend")
	}
	defer final.Release()

	if v, ok := final.Get(OutputKey("a")); !ok || v != "a" {
		t.Fatalf("preserved state lost a's output: v=%v ok=%v", v, ok)
	}
}
