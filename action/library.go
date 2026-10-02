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
	// Aliases is retained for compatibility with sibling packages. Registry
	// construction validates declarations and rejects malformed entries,
	// missing targets, duplicate names, and collisions.
	Aliases []Alias
	// Overrides names actions this library replaces. Each override must name an
	// action declared by this library and an action registered earlier.
	Overrides []string
}

// Alias maps one or more short names to a canonical action, source, or
// operator. The target and every short name must be unique in the completed
// registry; invalid or colliding aliases make construction fail.
type Alias struct {
	Canonical string
	Short     []string
}

// Of is the terse constructor used by tests and inline libraries.
func Of(actions ...AnyAction) Library {
	return Library{Name: "inline", Actions: actions}
}
