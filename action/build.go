package action

import (
	"fmt"
	"sort"
)

// hookClaimer is implemented by *BuiltAction. It lets NewRegistry detect
// that a caller is trying to apply library hooks to an action that already
// received them in a previous registry, which would silently duplicate them.
type hookClaimer interface {
	claimRegistryHooks() bool
}

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

	// Claim registry hooks once per action, before applying any. This prevents
	// silently doubling hooks when the same action instance is passed to a
	// second NewRegistry call — a documented but previously unenforced contract.
	hasHooks := false
	for i := range libs {
		if len(libs[i].Hooks) > 0 {
			hasHooks = true
			break
		}
	}

	if hasHooks {
		for _, c := range winners {
			claimer, ok := c.action.(hookClaimer)
			if !ok {
				// Custom AnyAction implementations are not tracked; they remain
				// reusable across registries as before.
				continue
			}
			if !claimer.claimRegistryHooks() {
				return nil, fmt.Errorf(
					"action: %q already received hooks from a previous NewRegistry call; "+
						"build a fresh action for the second registry",
					c.action.Describe().Name,
				)
			}
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

func MustNewRegistry(libs ...Library) *Registry {
	reg, err := NewRegistry(libs...)
	if err != nil {
		panic(err)
	}
	return reg
}
