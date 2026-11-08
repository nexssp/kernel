package dag

import "testing"

func TestStateSnapshotSharingAndDivergence(t *testing.T) {
	source := AcquireState()
	defer source.Release()
	source.Set("key", "before")

	read := source.AsRead()
	clone := source.Clone()
	defer clone.Release()
	copied := AcquireState()
	defer copied.Release()
	copied.CopyFrom(source)

	source.Set("key", "source update")
	for name, snapshot := range map[string]ReadState{
		"read view": read,
		"clone":     clone.AsRead(),
		"CopyFrom":  copied.AsRead(),
	} {
		if value, ok := snapshot.Get("key"); !ok || value != "before" {
			t.Errorf("%s value = (%v, %t), want (before, true)", name, value, ok)
		}
	}

	clone.Set("clone update", true)
	copied.Set("copy update", true)
	if value, ok := source.Get("key"); !ok || value != "source update" {
		t.Errorf("source value = (%v, %t), want (source update, true)", value, ok)
	}
	if _, ok := source.Get("clone update"); ok {
		t.Fatal("clone update appeared in source")
	}
	if _, ok := source.Get("copy update"); ok {
		t.Fatal("CopyFrom update appeared in source")
	}
}

func TestStateEmptyKeyNilValueAndShallowValues(t *testing.T) {
	type mutableValue struct{ count int }
	shared := &mutableValue{count: 1}
	state := AcquireState()
	defer state.Release()
	state.Set("", nil)
	state.Set("shared", shared)

	if value, ok := state.Get(""); !ok || value != nil {
		t.Fatalf("empty-key value = (%v, %t), want (nil, true)", value, ok)
	}
	clone := state.Clone()
	defer clone.Release()
	shared.count++
	value, ok := clone.Get("shared")
	if !ok {
		t.Fatal("clone lost the shared value")
	}
	mutable, ok := value.(*mutableValue)
	if !ok || mutable.count != 2 {
		t.Fatalf("clone did not retain the shallow value: (%v, %t)", value, ok)
	}

	data := state.Data()
	if value, ok := data[""]; !ok || value != nil {
		t.Fatalf("Data empty-key value = (%v, %t), want (nil, true)", value, ok)
	}
	data["added"] = true
	if _, ok := state.Get("added"); ok {
		t.Fatal("mutating Data result modified State")
	}
}

func TestStateNilReleaseAndReinitialization(t *testing.T) {
	var nilState *State
	nilState.Release()
	nilState.Set("ignored", true)
	if value, ok := nilState.Get("ignored"); ok || value != nil {
		t.Fatalf("nil State Get = (%v, %t), want (nil, false)", value, ok)
	}
	if nilState.Data() != nil || nilState.AsRead().Data() != nil {
		t.Fatal("nil State Data should be nil")
	}

	state := AcquireState()
	if state.Data() == nil || state.AsRead().Data() == nil {
		t.Fatal("empty State Data should be non-nil")
	}
	state.Set("key", "value")
	read := state.AsRead()
	state.Release()
	state.Release()
	if state.Data() != nil || state.AsRead().Data() != nil {
		t.Fatal("released State Data should be nil")
	}
	if value, ok := read.Get("key"); !ok || value != "value" {
		t.Fatalf("outstanding ReadState value = (%v, %t), want (value, true)", value, ok)
	}

	state.Set("again", "usable")
	if value, ok := state.Get("again"); !ok || value != "usable" {
		t.Fatalf("Set after Release = (%v, %t), want (usable, true)", value, ok)
	}
	if _, ok := state.Get("key"); ok {
		t.Fatal("Set after Release retained the released contents")
	}
	state.Release()
}

func TestCloneOfNilOrReleasedStateIsEmpty(t *testing.T) {
	var nilState *State
	clone := nilState.Clone()
	if clone == nil || clone.Data() == nil || len(clone.Data()) != 0 {
		t.Fatal("Clone of nil State should be a non-nil empty State")
	}
	clone.Release()

	state := AcquireState()
	state.Set("key", "value")
	state.Release()
	clone = state.Clone()
	defer clone.Release()
	if clone.Data() == nil || len(clone.Data()) != 0 {
		t.Fatal("Clone of released State should be empty")
	}
}

func TestCopyFromNilIsNoOp(t *testing.T) {
	target := AcquireState()
	defer target.Release()
	target.Set("keep", "value")
	target.CopyFrom(nil)
	if value, ok := target.Get("keep"); !ok || value != "value" {
		t.Fatalf("CopyFrom(nil) changed target: (%v, %t)", value, ok)
	}
	var nilTarget *State
	nilTarget.CopyFrom(target)
}
