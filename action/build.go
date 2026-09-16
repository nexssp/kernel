package action

import (
	"fmt"
	"sort"
)

// NewRegistry composes the given libraries into an immutable Registry.
//
// Rules:
//  1. Every action registers under its Describe().Name.
//  2. Nil actions are skipped; unnamed actions are rejected.
//  3. Duplicates within one library are rejected.
//  4. Duplicates across libraries are rejected unless the later library
//     lists the name in Overrides.
//  5. Hooks from every library apply, in library order, to every
//     surviving action before return.
//  6. A canonical name always wins over an alias; an earlier library
//     wins over a later one for the same alias short name.
//
// Callers must not share the same AnyAction across two registries built
// with different hooks: AddAnyHook mutates the action, so hooks would
// accumulate across calls.
func NewRegistry(libs ...Library) (*Registry, error) {
	if len(libs) == 0 {
		return &Registry{byName: make(map[string]AnyAction)}, nil
	}

	allowed := make(map[string]string, 16)
	for i := range libs {
		lib := &libs[i]
		for _, name := range lib.Overrides {
			if previous, duplicate := allowed[name]; duplicate {
				return nil, fmt.Errorf(
					"action: %q and %q both declare Overrides for %q",
					previous, lib.Name, name,
				)
			}
			allowed[name] = lib.Name
		}
	}

	type claim struct {
		library string
		action  AnyAction
	}

	winners := make(map[string]claim, 64)

	for i := range libs {
		lib := &libs[i]
		seen := make(map[string]bool, len(lib.Actions))

		for _, act := range lib.Actions {
			if act == nil {
				continue
			}
			meta := act.Describe()
			if meta == nil || meta.Name == "" {
				return nil, fmt.Errorf(
					"action: library %q contains an action with no name",
					lib.Name,
				)
			}
			if seen[meta.Name] {
				return nil, fmt.Errorf(
					"action: library %q declares %q more than once",
					lib.Name, meta.Name,
				)
			}
			seen[meta.Name] = true

			if previous, duplicate := winners[meta.Name]; duplicate {
				if allowed[meta.Name] != lib.Name {
					return nil, fmt.Errorf(
						"action: %q declared by both %q and %q; add %q to %s.Overrides to accept",
						meta.Name, previous.library, lib.Name, meta.Name, lib.Name,
					)
				}
			}
			winners[meta.Name] = claim{library: lib.Name, action: act}
		}
	}

	for i := range libs {
		if len(libs[i].Hooks) == 0 {
			continue
		}
		for _, c := range winners {
			c.action.AddAnyHook(libs[i].Hooks...)
		}
	}

	byName := make(map[string]AnyAction, len(winners)*2)
	for name, c := range winners {
		byName[name] = c.action
	}

	for i := range libs {
		lib := &libs[i]
		for _, alias := range lib.Aliases {
			canonical, exists := byName[alias.Canonical]
			if !exists {
				continue
			}
			for _, short := range alias.Short {
				if short == "" || short == alias.Canonical {
					continue
				}
				if _, taken := byName[short]; taken {
					continue
				}
				byName[short] = canonical
			}
		}
	}

	actions := make([]AnyAction, 0, len(winners))
	for _, c := range winners {
		actions = append(actions, c.action)
	}
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].Describe().Name < actions[j].Describe().Name
	})

	return &Registry{byName: byName, actions: actions}, nil
}
