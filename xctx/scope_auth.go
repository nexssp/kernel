package xctx

import (
	"context"
	"slices"
)

// HasRole reports whether the scope has the given role, either as its
// primary Role or anywhere in the Roles slice.
func HasRole(ctx context.Context, required string) bool {
	s := lockScopeForRead(ctx)
	if s == nil {
		return false
	}
	defer s.mu.RUnlock()
	return hasRole(s.Role, s.Roles, required)
}

// HasAnyRole reports whether the scope has any of the listed roles.
// Passing zero roles returns false.
func HasAnyRole(ctx context.Context, allowed ...string) bool {
	s := lockScopeForRead(ctx)
	if s == nil {
		return false
	}
	defer s.mu.RUnlock()
	for _, r := range allowed {
		if hasRole(s.Role, s.Roles, r) {
			return true
		}
	}
	return false
}

// HasPermission reports whether the scope grants perm or the wildcard "*".
func HasPermission(ctx context.Context, perm string) bool {
	s := lockScopeForRead(ctx)
	if s == nil {
		return false
	}
	defer s.mu.RUnlock()
	return hasPermission(s.Permissions, perm)
}

// HasFeature reports whether the scope has the given feature flag
// enabled. system_admin always passes; "all" and "*" are treated as
// wildcards.
func HasFeature(ctx context.Context, feature string) bool {
	s := lockScopeForRead(ctx)
	if s == nil {
		return false
	}
	defer s.mu.RUnlock()
	return hasFeature(s.Role, s.Roles, s.Features, feature)
}

// HasRole reports whether this snapshot contains the given role.
func (snapshot ScopeSnapshot) HasRole(required string) bool {
	return snapshot.Present && hasRole(snapshot.Role, snapshot.Roles, required)
}

// HasPermission reports whether this snapshot grants permission or the wildcard "*".
func (snapshot ScopeSnapshot) HasPermission(permission string) bool {
	return snapshot.Present && hasPermission(snapshot.Permissions, permission)
}

// HasFeature reports whether this snapshot enables feature, including admin and wildcard rules.
func (snapshot ScopeSnapshot) HasFeature(feature string) bool {
	return snapshot.Present && hasFeature(snapshot.Role, snapshot.Roles, snapshot.Features, feature)
}

func hasRole(role string, roles []string, required string) bool {
	return role == required || slices.Contains(roles, required)
}

func hasPermission(permissions []string, permission string) bool {
	for _, candidate := range permissions {
		if candidate == permission || candidate == "*" {
			return true
		}
	}
	return false
}

func hasFeature(role string, roles, features []string, feature string) bool {
	if hasRole(role, roles, "system_admin") {
		return true
	}
	for _, candidate := range features {
		if candidate == feature || candidate == "all" || candidate == "*" {
			return true
		}
	}
	return false
}
