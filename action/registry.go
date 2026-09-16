package action

// Registry is an immutable, deterministic snapshot of the actions
// declared by one or more Library values. Only NewRegistry produces one.
//
// Safe for unlimited concurrent reads: no lock is taken because the
// Registry never mutates after NewRegistry returns.
//
// Actions returns canonical actions in name-sorted order. Aliases are
// resolved by Get but do not appear in Actions.
type Registry struct {
	byName  map[string]AnyAction
	actions []AnyAction
}

// Get returns the action registered under name, where name may be a
// canonical action name or an alias short name.
func (r *Registry) Get(name string) (AnyAction, bool) {
	if r == nil {
		return nil, false
	}
	act, ok := r.byName[name]
	return act, ok
}

// Actions returns canonical actions in name-sorted order. The slice is
// owned by the Registry; callers must not mutate it.
func (r *Registry) Actions() []AnyAction {
	if r == nil {
		return nil
	}
	return r.actions
}

// Len returns the number of canonical actions. Aliases are not counted.
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	return len(r.actions)
}
