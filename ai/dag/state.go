package dag

import "maps"

// State is the mutable graph state. Only the DAG engine writes to it;
// nodes receive a read-only view via NodeContext.Input.
//
// Ownership: whoever receives a *State from Execute owns it and calls
// Release when finished. Release drops this State's reference to its
// backing map; any outstanding ReadState view keeps the map alive. The
// GC reclaims the map once every reference is gone.
//
// State is deliberately NOT pooled. Nodes receive NodeContext.Input and
// may legally retain that reference inside their own scope; recycling
// States across executions would create use-after-free hazards the
// compiler cannot catch.
type State struct {
	data map[string]any
}

// ReadState is the read-only view of a State handed to nodes. It shares
// the backing map with the origin State; copying a ReadState is free
// (one map-header copy) and keeps the map alive.
type ReadState struct {
	data map[string]any
}

// AcquireState returns a fresh, empty State. The caller owns it and
// should call Release when finished.
func AcquireState() *State {
	return &State{data: make(map[string]any, 16)}
}

// Release drops this State's reference to its backing map. It is a
// hint, not a lifetime operation: State is not pooled, so a forgotten
// Release is not a leak — the GC reclaims the map once every *State
// and ReadState view goes out of scope. Outstanding ReadState views
// (a node that captured Input into a goroutine) keep the map readable
// regardless of how many times Release has been called.
//
// Safe on a nil receiver and safe to call multiple times.
func (s *State) Release() {
	if s == nil {
		return
	}
	s.data = nil
}

// AsRead returns the read-only view of this state. Zero cost: it copies
// the map header. Safe on a nil receiver.
func (s *State) AsRead() ReadState {
	if s == nil {
		return ReadState{}
	}
	return ReadState{data: s.data}
}

// Clone returns a fresh *State with a shallow copy of the read view's
// backing map. Execute uses it to derive the layer's mutable state from
// the caller's initial snapshot.
func (r ReadState) Clone() *State {
	next := AcquireState()
	if r.data != nil {
		maps.Copy(next.data, r.data)
	}
	return next
}

// Get performs a zero-lock read on the read-only view.
func (r ReadState) Get(key string) (any, bool) {
	if r.data == nil {
		return nil, false
	}
	v, ok := r.data[key]
	return v, ok
}

// Data returns a shallow copy of the underlying map for serialization
// or rendering. The returned map is caller-owned.
func (r ReadState) Data() map[string]any {
	if r.data == nil {
		return nil
	}
	out := make(map[string]any, len(r.data))
	maps.Copy(out, r.data)
	return out
}

// Get on *State delegates to the read-only view so callers that hold the
// writable handle can read without an explicit AsRead hop. Nil-safe.
func (s *State) Get(key string) (any, bool) {
	if s == nil {
		return nil, false
	}
	return s.AsRead().Get(key)
}

// Data on *State delegates to the read-only view. Nil-safe.
func (s *State) Data() map[string]any {
	if s == nil {
		return nil
	}
	return s.AsRead().Data()
}

// Set writes a key-value pair to the state. Mutates the underlying map,
// which is shared with every ReadState view of this state. Only the DAG
// engine should call this.
func (s *State) Set(key string, val any) {
	if s == nil {
		return
	}
	if s.data == nil {
		s.data = make(map[string]any, 16)
	}
	s.data[key] = val
}

// Clone returns a fresh State with a shallow copy of the map. Values are
// shared, not deep-copied; nodes must not return mutable values they
// intend to modify after returning.
func (s *State) Clone() *State {
	next := AcquireState()
	if s != nil && s.data != nil {
		maps.Copy(next.data, s.data)
	}
	return next
}

// CopyFrom replaces s's data with a shallow copy of src's data.
func (s *State) CopyFrom(src *State) {
	if s == nil || src == nil {
		return
	}
	if s.data == nil {
		s.data = make(map[string]any, len(src.data))
	} else {
		clear(s.data)
	}
	if src.data != nil {
		maps.Copy(s.data, src.data)
	}
}
