package dag

import "github.com/arnonrgo/immutable"

// State is the mutable graph state. Only the DAG engine writes to it;
// nodes receive a read-only view via NodeContext.Input.
//
// Ownership: whoever receives a *State from Execute owns it and calls
// Release when finished. Release drops this State's root; any outstanding
// ReadState view keeps its immutable snapshot alive. The GC reclaims tree
// nodes once every State and ReadState reference to them is gone.
//
// State is deliberately NOT pooled. Nodes receive NodeContext.Input and
// may legally retain that reference inside their own scope; recycling
// States across executions would create use-after-free hazards the
// compiler cannot catch. Persistent roots preserve those snapshots while
// dynamic execution publishes later outputs.
type State struct {
	data *immutable.Map[string, any]
}

// ReadState is the read-only view of an immutable State snapshot. Copying
// it shares the root and keeps that snapshot alive.
type ReadState struct {
	data *immutable.Map[string, any]
}

// AcquireState returns a fresh, empty State. The caller owns it and
// should call Release when finished.
func AcquireState() *State {
	return &State{data: immutable.NewMap[string, any](nil)}
}

// Release drops this State's reference to its immutable root. Outstanding
// ReadState views keep their own roots alive, regardless of how many times
// Release has been called.
//
// Safe on a nil receiver and safe to call multiple times.
func (s *State) Release() {
	if s == nil {
		return
	}
	s.data = nil
}

// AsRead returns the immutable snapshot of this state. Zero cost: it copies
// the root pointer. Safe on a nil receiver.
func (s *State) AsRead() ReadState {
	if s == nil {
		return ReadState{}
	}
	return ReadState{data: s.data}
}

// Clone returns a fresh *State sharing the immutable root of the read view.
func (r ReadState) Clone() *State {
	if r.data == nil {
		return AcquireState()
	}
	return &State{data: r.data}
}

// Get performs a zero-lock read on the immutable snapshot.
func (r ReadState) Get(key string) (any, bool) {
	if r.data == nil {
		return nil, false
	}
	return r.data.Get(key)
}

// Data returns a shallow copy of the snapshot for serialization
// or rendering. The returned map is caller-owned.
func (r ReadState) Data() map[string]any {
	if r.data == nil {
		return nil
	}
	out := make(map[string]any, r.data.Len())
	iterator := r.data.Iterator()
	for key, value, ok := iterator.Next(); ok; key, value, ok = iterator.Next() {
		out[key] = value
	}
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

// Set writes a key-value pair by replacing the persistent root. Existing
// ReadState snapshots remain unchanged. Only the DAG engine should call this.
func (s *State) Set(key string, val any) {
	if s == nil {
		return
	}
	if s.data == nil {
		s.data = immutable.NewMap[string, any](nil)
	}
	s.data = s.data.Set(key, val)
}

// Clone returns a fresh State with the same immutable root. Values are
// shared, not deep-copied; nodes must not return mutable values they
// intend to modify after returning.
func (s *State) Clone() *State {
	if s != nil && s.data != nil {
		return &State{data: s.data}
	}
	return AcquireState()
}

// CopyFrom shares src's immutable root. Later Set calls replace only the
// receiver's root, so source and destination diverge after either is written.
func (s *State) CopyFrom(src *State) {
	if s == nil || src == nil {
		return
	}
	if src.data == nil {
		s.data = immutable.NewMap[string, any](nil)
		return
	}
	s.data = src.data
}
