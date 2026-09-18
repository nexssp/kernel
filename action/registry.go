// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

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

	allowed, err := buildOverridesMap(libs)
	if err != nil {
		return nil, err
	}

	winners, err := collectWinners(libs, allowed)
	if err != nil {
		return nil, err
	}

	if err := claimRegistryHooks(winners, libs); err != nil {
		return nil, err
	}

	applyRegistryHooks(winners, libs)
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

func claimRegistryHooks(winners map[string]winnerClaim, libs []Library) error {
	if !anyLibraryHasHooks(libs) {
		return nil
	}
	for _, c := range winners {
		claimer, ok := c.action.(hookClaimer)
		if !ok {
			continue
		}
		if !claimer.claimRegistryHooks() {
			return fmt.Errorf(
				"action: %q already received hooks from a previous NewRegistry call; "+
					"build a fresh action for the second registry",
				c.action.Describe().Name,
			)
		}
	}
	return nil
}

func anyLibraryHasHooks(libs []Library) bool {
	for i := range libs {
		if len(libs[i].Hooks) > 0 {
			return true
		}
	}
	return false
}

func applyRegistryHooks(winners map[string]winnerClaim, libs []Library) {
	for i := range libs {
		if len(libs[i].Hooks) == 0 {
			continue
		}
		for _, c := range winners {
			c.action.AddAnyHook(libs[i].Hooks...)
		}
	}
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
