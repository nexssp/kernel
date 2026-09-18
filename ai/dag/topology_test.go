package dag_test

import (
	"testing"

	"github.com/nexssp/kernel/ai/dag"
)

func TestTopologyRegistry_RegisterBounded(t *testing.T) {
	dag.GlobalTopology.Reset()
	for range 10000 {
		dag.GlobalTopology.Register("A", "B", "chan", "proto")
	}
	out := dag.GlobalTopology.ToMermaid()
	expected := "    id_A -- \"chan (proto)\" --> id_B\n"
	if out != expected {
		t.Fatalf("expected bounded output %q, got %q", expected, out)
	}
}

func TestToMermaid_Deterministic(t *testing.T) {
	dag.GlobalTopology.Reset()
	dag.GlobalTopology.Register("A", "B", "chan2", "proto")
	dag.GlobalTopology.Register("A", "B", "chan1", "proto")

	out1 := dag.GlobalTopology.ToMermaid()
	out2 := dag.GlobalTopology.ToMermaid()

	if out1 != out2 {
		t.Fatalf("output is not deterministic")
	}
	expected := "    id_A -- \"chan1 (proto)\" --> id_B\n    id_A -- \"chan2 (proto)\" --> id_B\n"
	if out1 != expected {
		t.Fatalf("expected %q, got %q", expected, out1)
	}
}
