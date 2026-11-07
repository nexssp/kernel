package dag

import "testing"

func TestCopyFromSharesPersistentRoot(t *testing.T) {
	source := AcquireState()
	defer source.Release()
	source.Set("key", "before")

	target := AcquireState()
	defer target.Release()
	target.CopyFrom(source)
	if target.data != source.data {
		t.Fatal("CopyFrom did not share the source root")
	}

	target.Set("key", "after")
	if target.data == source.data {
		t.Fatal("Set did not publish a separate root")
	}
	if value, ok := source.Get("key"); !ok || value != "before" {
		t.Fatalf("source value = (%v, %t), want (before, true)", value, ok)
	}
}
