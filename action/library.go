package action

// Library is a named, declarative contribution to the action set.
// Transports, domain packages, and applications each contribute one.
//
// A Library is a value. Compose two or more with NewRegistry.
type Library struct {
	Name        string
	Description string
	Actions     []AnyAction
	Hooks       []AnyHook
	Aliases     []Alias
	Overrides   []string
}

// Alias maps a canonical action name to short names a caller may type
// in its place.
type Alias struct {
	Canonical string
	Short     []string
}

// Of wraps actions as an unnamed inline Library. Use it to mix a small
// set of ad-hoc actions with named libraries in NewRegistry.
func Of(actions ...AnyAction) Library {
	return Library{Name: "inline", Actions: actions}
}
