package dag_test

import (
	"testing"

	"github.com/nexssp/kernel/ai/dag"
)

func TestPersistentStateSnapshotKeepsEmptyKeyAndNilValue(t *testing.T) {
	state := dag.AcquireState()
	defer state.Release()
	state.Set("", nil)

	snapshot := state.AsRead()
	clone := snapshot.Clone()
	defer clone.Release()
	state.Set("later", "value")

	if value, ok := snapshot.Get(""); !ok || value != nil {
		t.Fatalf("snapshot empty key = (%v, %t), want (nil, true)", value, ok)
	}
	if _, ok := snapshot.Get("later"); ok {
		t.Fatal("snapshot observed a later state write")
	}
	if _, ok := clone.Get("later"); ok {
		t.Fatal("cloned snapshot observed a later state write")
	}
	if data := snapshot.Data(); len(data) != 1 || data[""] != nil {
		t.Fatalf("snapshot data = %#v, want one empty-key nil value", data)
	}
}

func TestStateCopyFromSharesRootAndDivergesOnSet(t *testing.T) {
	source := dag.AcquireState()
	defer source.Release()
	source.Set("shared", "initial")

	target := dag.AcquireState()
	defer target.Release()
	target.CopyFrom(source)

	target.Set("shared", "target")
	target.Set("target-only", true)
	if value, ok := source.Get("shared"); !ok || value != "initial" {
		t.Fatalf("source shared value = (%v, %t), want (initial, true)", value, ok)
	}
	if _, ok := source.Get("target-only"); ok {
		t.Fatal("source observed a target-only write after CopyFrom")
	}

	source.Set("source-only", true)
	if value, ok := target.Get("shared"); !ok || value != "target" {
		t.Fatalf("target shared value = (%v, %t), want (target, true)", value, ok)
	}
	if _, ok := target.Get("source-only"); ok {
		t.Fatal("target observed a source-only write after CopyFrom")
	}
}
