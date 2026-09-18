package xctx

import (
	"context"
	"slices"
)

// HasRole reports whether the scope has the given role, either as its
// primary Role or anywhere in the Roles slice.
func HasRole(ctx context.Context, required string) bool {
	s := ScopeFrom(ctx)
	if s == nil {
		return false
	}
	if s.Role == required {
		return true
	}
	return slices.Contains(s.Roles, required)
}

// HasAnyRole reports whether the scope has any of the listed roles.
// Passing zero roles returns false.
func HasAnyRole(ctx context.Context, allowed ...string) bool {
	s := ScopeFrom(ctx)
	if s == nil {
		return false
	}
	for _, r := range allowed {
		if s.Role == r || slices.Contains(s.Roles, r) {
			return true
		}
	}
	return false
}

// HasPermission reports whether the scope grants perm or the wildcard "*".
func HasPermission(ctx context.Context, perm string) bool {
	s := ScopeFrom(ctx)
	if s == nil {
		return false
	}
	for _, p := range s.Permissions {
		if p == perm || p == "*" {
			return true
		}
	}
	return false
}

// HasFeature reports whether the scope has the given feature flag
// enabled. system_admin always passes; "all" and "*" are treated as
// wildcards.
func HasFeature(ctx context.Context, feature string) bool {
	if HasRole(ctx, "system_admin") {
		return true
	}
	s := ScopeFrom(ctx)
	if s == nil {
		return false
	}
	for _, f := range s.Features {
		if f == feature || f == "all" || f == "*" {
			return true
		}
	}
	return false
}
