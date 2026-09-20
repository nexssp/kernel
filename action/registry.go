package action

import (
	"fmt"
	"sort"
)

// NewRegistry assembles a set of libraries into a single immutable
// lookup table.
//
// Registries never mutate actions. Every hook and every binding an
// action will ever have must be attached before Build(). This keeps
// NewRegistry pure and makes an action safe to reuse in any number of
// registries.
func NewRegistry(libs ...Library) (*Registry, error) {
	if len(libs) == 0 {
		return &Registry{byName: make(map[string]AnyAction)}, nil
	}

	allowed, err := buildOverridesMap(libs)
	if err != nil {
		return nil, err
	}

	winners, err := collectWinners(libs, allowed)
	if err != nil {
		return nil, err
	}

	byName := indexWinners(winners)
	applyAliases(byName, libs)

	return &Registry{
		byName:  byName,
		actions: sortedWinners(winners),
	}, nil
}

func buildOverridesMap(libs []Library) (map[string]string, error) {
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
	return allowed, nil
}

type winnerClaim struct {
	library string
	action  AnyAction
}

func collectWinners(libs []Library, allowed map[string]string) (map[string]winnerClaim, error) {
	winners := make(map[string]winnerClaim, 64)
	for i := range libs {
		lib := &libs[i]
		seen := make(map[string]bool, len(lib.Actions))
		for _, act := range lib.Actions {
			if err := collectOneAction(lib.Name, act, seen, winners, allowed); err != nil {
				return nil, err
			}
		}
	}
	return winners, nil
}

func collectOneAction(
	libName string,
	act AnyAction,
	seen map[string]bool,
	winners map[string]winnerClaim,
	allowed map[string]string,
) error {
	if act == nil {
		return nil
	}
	meta := act.Describe()
	if meta == nil || meta.Name == "" {
		return fmt.Errorf("action: library %q contains an action with no name", libName)
	}
	if seen[meta.Name] {
		return fmt.Errorf("action: library %q declares %q more than once", libName, meta.Name)
	}
	seen[meta.Name] = true

	if previous, duplicate := winners[meta.Name]; duplicate {
		if allowed[meta.Name] != libName {
			return fmt.Errorf(
				"action: %q declared by both %q and %q; add %q to %s.Overrides to accept",
				meta.Name, previous.library, libName, meta.Name, libName,
			)
		}
	}
	winners[meta.Name] = winnerClaim{library: libName, action: act}
	return nil
}

func indexWinners(winners map[string]winnerClaim) map[string]AnyAction {
	byName := make(map[string]AnyAction, len(winners)*2)
	for name, c := range winners {
		byName[name] = c.action
	}
	return byName
}

func applyAliases(byName map[string]AnyAction, libs []Library) {
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
}

func sortedWinners(winners map[string]winnerClaim) []AnyAction {
	actions := make([]AnyAction, 0, len(winners))
	for _, c := range winners {
		actions = append(actions, c.action)
	}
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].Describe().Name < actions[j].Describe().Name
	})
	return actions
}

func MustNewRegistry(libs ...Library) *Registry {
	reg, err := NewRegistry(libs...)
	if err != nil {
		panic(err)
	}
	return reg
}

// Library is a named group of actions.
//
// There is no Hooks field. Cross-cutting behavior is attached to each
// action before Build() via .AnyHook(...). This keeps registries pure
// and lets the same action appear in any number of registries.
type Library struct {
	Name        string
	Description string
	Actions     []AnyAction
	Aliases     []Alias
	Overrides   []string
}

type Alias struct {
	Canonical string
	Short     []string
}

func Of(actions ...AnyAction) Library {
	return Library{Name: "inline", Actions: actions}
}

type Registry struct {
	byName  map[string]AnyAction
	actions []AnyAction
}

func (r *Registry) Get(name string) (AnyAction, bool) {
	if r == nil {
		return nil, false
	}
	act, ok := r.byName[name]
	return act, ok
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
