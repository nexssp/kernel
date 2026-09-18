package dag_test

import (
	"testing"

	"github.com/nexssp/kernel/ai/dag"
	"github.com/nexssp/kernel/xerr"
)

type DummyResult struct {
	Status string `json:"status"`
	Value  int    `json:"value"`
}

func TestGetNodeOutput_CoercionAndBadPaths(t *testing.T) {
	t.Run("1. Hot Path - direct type assertion", func(t *testing.T) {
		state := dag.AcquireState()
		defer state.Release()

		state.Set(dag.OutputKey("node_a"), DummyResult{Status: "ok", Value: 42})

		res, err := dag.GetNodeOutput[DummyResult](state.AsRead(), "node_a")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if res.Value != 42 {
			t.Fatalf("expected 42, got %d", res.Value)
		}
	})

	t.Run("2. Resume Path - coerce from JSON map", func(t *testing.T) {
		state := dag.AcquireState()
		defer state.Release()

		state.Set(dag.OutputKey("node_b"), map[string]any{
			"status": "resumed",
			"value":  float64(99),
		})

		res, err := dag.GetNodeOutput[DummyResult](state.AsRead(), "node_b")
		if err != nil {
			t.Fatalf("expected successful coercion, got: %v", err)
		}
		if res.Status != "resumed" || res.Value != 99 {
			t.Fatalf("coercion failed to map fields correctly: %+v", res)
		}
	})

	t.Run("3. Bad Path - invalid coercion target", func(t *testing.T) {
		state := dag.AcquireState()
		defer state.Release()

		state.Set(dag.OutputKey("node_c"), "this is just a string")

		_, err := dag.GetNodeOutput[DummyResult](state.AsRead(), "node_c")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if xerr.KindFrom(err) != xerr.KindValidation {
			t.Fatalf("expected KindValidation, got: %s (err: %v)", xerr.KindFrom(err), err)
		}
	})

	t.Run("4. Bad Path - missing key", func(t *testing.T) {
		state := dag.AcquireState()
		defer state.Release()

		_, err := dag.GetNodeOutput[DummyResult](state.AsRead(), "ghost_node")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if xerr.KindFrom(err) != xerr.KindNotFound {
			t.Fatalf("expected KindNotFound, got: %s", xerr.KindFrom(err))
		}
	})

	t.Run("5. Bad Path - zero ReadState", func(t *testing.T) {
		var state dag.ReadState

		_, err := dag.GetNodeOutput[DummyResult](state, "node_x")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if xerr.KindFrom(err) != xerr.KindInternal {
			t.Fatalf("expected KindInternal, got: %s", xerr.KindFrom(err))
		}
	})

	t.Run("6. Hot path - direct assertion has zero allocations", func(t *testing.T) {
		state := dag.AcquireState()
		defer state.Release()
		state.Set(dag.OutputKey("node_hot"), DummyResult{Status: "ok", Value: 7})
		view := state.AsRead()

		allocs := testing.AllocsPerRun(1000, func() {
			if _, err := dag.GetNodeOutput[DummyResult](view, "node_hot"); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 {
			t.Fatalf("hot path must be zero-alloc, got %.1f", allocs)
		}
	})
}
