package action

// Library is a named group of actions, sources, and operators.
//
// Hooks declared here are applied to every Action and every Source at
// Register time via CloneWithHooks. The originals are never mutated;
// the registry holds private clones. Operators have no lifecycle, so
// Library-level hooks do not apply to them.
type Library struct {
	Name        string
	Description string
	Actions     []AnyAction
	Sources     []AnyStreamAction
	Operators   []NamedOperator
	Hooks       []AnyHook
	Aliases     []Alias
	Overrides   []string // Used by test harnesses to shadow existing actions
}

// Alias maps a short name to a canonical name. Canonical must already
// resolve to a registered action, source, or operator; Short names are
// registered as aliases pointing at the same entry.
type Alias struct {
	Canonical string
	Short     []string
}

// Of is the terse constructor used by tests and inline libraries.
func Of(actions ...AnyAction) Library {
	return Library{Name: "inline", Actions: actions}
}
