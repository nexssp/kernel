package action

import (
	"fmt"
	"slices"
	"strings"
)

const (
	registryActionKind   = "action"
	registrySourceKind   = "source"
	registryOperatorKind = "operator"
)

type registryNameOwner struct {
	kind    string
	library string
}

// validateRegistryLibraries checks the complete input before NewRegistry starts
// building its lookup maps. Alias registration is therefore all-or-nothing.
func validateRegistryLibraries(libs []Library) error {
	owners, err := validateCanonicalLibraries(libs)
	if err != nil {
		return err
	}
	return validateRegistryAliases(libs, owners)
}

func validateCanonicalLibraries(libs []Library) (map[string]registryNameOwner, error) {
	owners := make(map[string]registryNameOwner)
	for i := range libs {
		lib := &libs[i]
		seen := make(map[string]string)
		if err := validateLibraryOverrides(lib, owners); err != nil {
			return nil, err
		}
		if err := validateLibraryActions(lib, seen, owners); err != nil {
			return nil, err
		}
		if err := validateLibrarySources(lib, seen, owners); err != nil {
			return nil, err
		}
		if err := validateLibraryOperators(lib, seen, owners); err != nil {
			return nil, err
		}
	}
	return owners, nil
}

func validateLibraryOverrides(lib *Library, owners map[string]registryNameOwner) error {
	declaredActions := make(map[string]struct{}, len(lib.Actions))
	for _, act := range lib.Actions {
		if act == nil {
			continue
		}
		meta := act.Describe()
		if meta != nil {
			declaredActions[meta.Name] = struct{}{}
		}
	}

	seen := make(map[string]struct{}, len(lib.Overrides))
	for _, name := range lib.Overrides {
		if !validRegistryName(name) {
			return fmt.Errorf("library %q declares an override with no valid action name", lib.Name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("library %q declares override for action %q more than once", lib.Name, name)
		}
		seen[name] = struct{}{}
		if _, declared := declaredActions[name]; !declared {
			return fmt.Errorf("library %q declares override for action %q but does not declare that action", lib.Name, name)
		}
		previous, exists := owners[name]
		if !exists {
			return fmt.Errorf("library %q overrides action %q without a previously registered target", lib.Name, name)
		}
		if previous.kind != registryActionKind {
			return registryKindCollision(name, registryActionKind, lib.Name, previous)
		}
	}
	return nil
}

func validateLibraryActions(lib *Library, seen map[string]string, owners map[string]registryNameOwner) error {
	for _, act := range lib.Actions {
		if act == nil {
			continue
		}
		meta := act.Describe()
		if meta == nil || !validRegistryName(meta.Name) {
			return fmt.Errorf("library %q contains an action with no name", lib.Name)
		}
		if kind, ok := seen[meta.Name]; ok {
			if kind == registryActionKind {
				return fmt.Errorf("library %q declares %q more than once", lib.Name, meta.Name)
			}
			return fmt.Errorf("library %q declares %s and action %q with the same name", lib.Name, kind, meta.Name)
		}
		seen[meta.Name] = registryActionKind
		if previous, ok := owners[meta.Name]; ok {
			if previous.kind != registryActionKind {
				return registryKindCollision(meta.Name, registryActionKind, lib.Name, previous)
			}
			if previous.library == lib.Name || !slices.Contains(lib.Overrides, meta.Name) {
				return fmt.Errorf("action: %q declared by both %q and %q", meta.Name, previous.library, lib.Name)
			}
		}
		owners[meta.Name] = registryNameOwner{kind: registryActionKind, library: lib.Name}
	}
	return nil
}

func validateLibrarySources(lib *Library, seen map[string]string, owners map[string]registryNameOwner) error {
	for _, src := range lib.Sources {
		if src == nil {
			continue
		}
		meta := src.Describe()
		if meta == nil || !validRegistryName(meta.Name) {
			return fmt.Errorf("library %q contains a source with no valid name", lib.Name)
		}
		if kind, ok := seen[meta.Name]; ok {
			return fmt.Errorf("library %q declares %s and source %q with the same name", lib.Name, kind, meta.Name)
		}
		seen[meta.Name] = registrySourceKind
		if previous, ok := owners[meta.Name]; ok {
			if previous.kind != registrySourceKind {
				return registryKindCollision(meta.Name, registrySourceKind, lib.Name, previous)
			}
			return fmt.Errorf("source: %q declared by both %q and %q", meta.Name, previous.library, lib.Name)
		}
		owners[meta.Name] = registryNameOwner{kind: registrySourceKind, library: lib.Name}
	}
	return nil
}

func validateLibraryOperators(lib *Library, seen map[string]string, owners map[string]registryNameOwner) error {
	for _, op := range lib.Operators {
		if err := ValidateOperatorDeclaration(op); err != nil {
			return fmt.Errorf("library %q: %w", lib.Name, err)
		}
		if !validRegistryName(op.Name) {
			return fmt.Errorf("library %q: operator %q has an invalid name", lib.Name, op.Name)
		}
		if kind, ok := seen[op.Name]; ok {
			return fmt.Errorf("library %q declares %s and operator %q with the same name", lib.Name, kind, op.Name)
		}
		seen[op.Name] = registryOperatorKind
		if previous, ok := owners[op.Name]; ok {
			if previous.kind != registryOperatorKind {
				return registryKindCollision(op.Name, registryOperatorKind, lib.Name, previous)
			}
			return fmt.Errorf("operator: %q declared by both %q and %q", op.Name, previous.library, lib.Name)
		}
		owners[op.Name] = registryNameOwner{kind: registryOperatorKind, library: lib.Name}
	}
	return nil
}

func validateRegistryAliases(libs []Library, canonicalOwners map[string]registryNameOwner) error {
	aliasOwners := make(map[string]registryNameOwner)
	for i := range libs {
		lib := &libs[i]
		for _, alias := range lib.Aliases {
			if err := validateRegistryAlias(lib.Name, alias, canonicalOwners, aliasOwners); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRegistryAlias(library string, alias Alias, canonicalOwners, aliasOwners map[string]registryNameOwner) error {
	if !validRegistryName(alias.Canonical) || len(alias.Short) == 0 {
		return fmt.Errorf("malformed alias in library %q: canonical %q must be valid and at least one short name is required",
			library, alias.Canonical)
	}
	targetOwner, ok := canonicalOwners[alias.Canonical]
	if !ok {
		return fmt.Errorf("alias in library %q targets missing canonical %q (kind canonical, owner unknown)", library, alias.Canonical)
	}
	for _, short := range alias.Short {
		if err := validateAliasName(library, alias.Canonical, short, targetOwner, canonicalOwners, aliasOwners); err != nil {
			return err
		}
	}
	return nil
}

func validateAliasName(library, canonical, short string, targetOwner registryNameOwner, canonicalOwners, aliasOwners map[string]registryNameOwner) error {
	if !validRegistryName(short) {
		return fmt.Errorf("malformed alias name %q in library %q for canonical %q (target kind %s, owner %q)",
			short, library, canonical, targetOwner.kind, targetOwner.library)
	}
	if short == canonical {
		return fmt.Errorf("malformed alias name %q in library %q: it repeats canonical kind %s owner %q",
			short, library, targetOwner.kind, targetOwner.library)
	}
	if canonicalOwner, exists := canonicalOwners[short]; exists {
		return fmt.Errorf("alias collision: name %q kind alias owner %q conflicts with canonical kind %s owner %q",
			short, library, canonicalOwner.kind, canonicalOwner.library)
	}
	if previous, exists := aliasOwners[short]; exists {
		return fmt.Errorf("duplicate alias name %q kind alias owners %q and %q (targets kind %s owner %q and kind %s owner %q)",
			short, previous.library, library, previous.kind, previous.library, targetOwner.kind, targetOwner.library)
	}
	aliasOwners[short] = registryNameOwner{kind: targetOwner.kind, library: library}
	return nil
}

func validRegistryName(name string) bool {
	return strings.TrimSpace(name) != "" && strings.TrimSpace(name) == name
}

func registryKindCollision(name, kind, library string, previous registryNameOwner) error {
	return fmt.Errorf("canonical collision: name %q kind %s owner %q conflicts with kind %s owner %q",
		name, kind, library, previous.kind, previous.library)
}
