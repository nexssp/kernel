package action

import (
	"fmt"
	"slices"
)

// Registry is an immutable lookup table assembled from one or more
// Library values. It never mutates actions: hooks and bindings must be
// attached before Build(), and library-level hooks are applied via
// CloneWithHooks so originals stay clean.
type Registry struct {
	byName     map[string]AnyAction
	byStream   map[string]AnyStreamAction
	byOperator map[string]NamedOperator
	actions    []AnyAction
}

// NewRegistry assembles the given libraries into a single Registry.
func NewRegistry(libs ...Library) (*Registry, error) {
	r := &Registry{
		byName:     make(map[string]AnyAction),
		byStream:   make(map[string]AnyStreamAction),
		byOperator: make(map[string]NamedOperator),
	}

	actionOwners := make(map[string]string)
	sourceOwners := make(map[string]string)
	operatorOwners := make(map[string]string)

	for i := range libs {
		lib := &libs[i]
		if err := r.registerActions(lib, actionOwners); err != nil {
			return nil, err
		}
		if err := r.registerSources(lib, sourceOwners); err != nil {
			return nil, err
		}
		if err := r.registerOperators(lib, operatorOwners); err != nil {
			return nil, err
		}
	}

	r.resolveAliases(libs)

	return r, nil
}

func (r *Registry) registerActions(lib *Library, owners map[string]string) error {
	seen := make(map[string]bool, len(lib.Actions))
	for _, rawAct := range lib.Actions {
		if rawAct == nil {
			continue
		}
		act := rawAct
		if len(lib.Hooks) > 0 {
			act = act.CloneWithHooks(lib.Hooks...)
		}
		meta := act.Describe()
		if meta == nil || meta.Name == "" {
			return fmt.Errorf("library %q contains an action with no name", lib.Name)
		}
		if seen[meta.Name] {
			return fmt.Errorf("library %q declares %q more than once", lib.Name, meta.Name)
		}

		isOverride := slices.Contains(lib.Overrides, meta.Name)
		if !isOverride {
			if owner, dup := owners[meta.Name]; dup {
				return fmt.Errorf("action: %q declared by both %q and %q", meta.Name, owner, lib.Name)
			}
		}

		seen[meta.Name] = true
		owners[meta.Name] = lib.Name
		r.byName[meta.Name] = act

		if isOverride {
			for i, existing := range r.actions {
				if existing != nil && existing.Describe() != nil && existing.Describe().Name == meta.Name {
					r.actions[i] = act
					break
				}
			}
		} else {
			r.actions = append(r.actions, act)
		}
	}
	return nil
}

func (r *Registry) registerSources(lib *Library, owners map[string]string) error {
	seen := make(map[string]bool, len(lib.Sources))
	for _, rawSrc := range lib.Sources {
		if rawSrc == nil {
			continue
		}
		src := rawSrc
		if len(lib.Hooks) > 0 {
			src = src.CloneWithHooks(lib.Hooks...)
		}
		meta := src.Describe()
		if meta == nil || meta.Name == "" {
			return fmt.Errorf("library %q contains a source with no name", lib.Name)
		}
		if seen[meta.Name] {
			return fmt.Errorf("library %q declares source %q more than once", lib.Name, meta.Name)
		}
		if owner, dup := owners[meta.Name]; dup {
			return fmt.Errorf("source: %q declared by both %q and %q", meta.Name, owner, lib.Name)
		}
		seen[meta.Name] = true
		owners[meta.Name] = lib.Name
		r.byStream[meta.Name] = src
	}
	return nil
}

func (r *Registry) registerOperators(lib *Library, owners map[string]string) error {
	seen := make(map[string]bool, len(lib.Operators))
	for _, op := range lib.Operators {
		if err := ValidateOperatorDeclaration(op); err != nil {
			return fmt.Errorf("library %q: %w", lib.Name, err)
		}
		if seen[op.Name] {
			return fmt.Errorf("library %q declares operator %q more than once", lib.Name, op.Name)
		}
		if owner, dup := owners[op.Name]; dup {
			return fmt.Errorf("operator: %q declared by both %q and %q", op.Name, owner, lib.Name)
		}
		seen[op.Name] = true
		owners[op.Name] = lib.Name
		r.byOperator[op.Name] = op.Clone()
	}
	return nil
}

func (r *Registry) resolveAliases(libs []Library) {
	for i := range libs {
		lib := &libs[i]
		for _, alias := range lib.Aliases {
			if alias.Canonical == "" || len(alias.Short) == 0 {
				continue
			}
			target, ok := r.lookupAny(alias.Canonical)
			if !ok {
				continue
			}
			for _, short := range alias.Short {
				if short == "" || short == alias.Canonical {
					continue
				}
				if _, exists := r.lookupAny(short); exists {
					continue
				}
				r.registerAlias(short, target)
			}
		}
	}
}

func MustNewRegistry(libs ...Library) *Registry {
	reg, err := NewRegistry(libs...)
	if err != nil {
		panic(err)
	}
	return reg
}

func (r *Registry) Get(name string) (AnyAction, bool) {
	if r == nil {
		return nil, false
	}
	act, ok := r.byName[name]
	return act, ok
}

func (r *Registry) GetStream(name string) (AnyStreamAction, bool) {
	if r == nil {
		return nil, false
	}
	src, ok := r.byStream[name]
	return src, ok
}

func (r *Registry) GetOperator(name string) (NamedOperator, bool) {
	if r == nil {
		return NamedOperator{}, false
	}
	op, ok := r.byOperator[name]
	if !ok {
		return NamedOperator{}, false
	}
	return op.Clone(), true
}

func (r *Registry) Actions() []AnyAction {
	if r == nil {
		return nil
	}
	return r.actions
}

func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	return len(r.actions)
}

func (r *Registry) lookupAny(name string) (any, bool) {
	if a, ok := r.byName[name]; ok {
		return a, true
	}
	if s, ok := r.byStream[name]; ok {
		return s, true
	}
	if op, ok := r.byOperator[name]; ok {
		return op, true
	}
	return nil, false
}

func (r *Registry) registerAlias(short string, target any) {
	switch v := target.(type) {
	case AnyAction:
		r.byName[short] = v
	case AnyStreamAction:
		r.byStream[short] = v
	case NamedOperator:
		r.byOperator[short] = v
	}
}

func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}

	seen := make(map[string]struct{})
	for n := range r.byName {
		seen[n] = struct{}{}
	}
	for n := range r.byStream {
		seen[n] = struct{}{}
	}
	for n := range r.byOperator {
		seen[n] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	slices.Sort(out)

	return out
}
