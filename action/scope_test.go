package action

import (
	"context"
	"testing"
)

func TestActionScopePredicates(t *testing.T) {
	tests := []struct {
		name     string
		meta     *Meta
		public   bool
		internal bool
		system   bool
	}{
		{name: "nil", meta: nil},
		{name: "zero value is public", meta: &Meta{}, public: true},
		{name: "explicit public", meta: &Meta{Scope: ScopePublic}, public: true},
		{name: "internal", meta: &Meta{Scope: ScopeInternal}, internal: true},
		{name: "system", meta: &Meta{Scope: ScopeSystem}, system: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.meta.IsPublic(); got != tt.public {
				t.Errorf("IsPublic() = %v, want %v", got, tt.public)
			}
			if got := tt.meta.IsInternal(); got != tt.internal {
				t.Errorf("IsInternal() = %v, want %v", got, tt.internal)
			}
			if got := tt.meta.IsSystem(); got != tt.system {
				t.Errorf("IsSystem() = %v, want %v", got, tt.system)
			}
		})
	}
}

func TestBuilderScopeMethodsAndProfiles(t *testing.T) {
	handler := func(context.Context, struct{}) (MessageRes, error) { return MessageRes{}, nil }

	if got := New("public", handler).Public().Build().Describe().Scope; got != ScopePublic {
		t.Fatalf("Public() scope = %q, want %q", got, ScopePublic)
	}
	if got := New("internal", handler).Internal().Build().Describe().Scope; got != ScopeInternal {
		t.Fatalf("Internal() scope = %q, want %q", got, ScopeInternal)
	}
	if got := New("system", handler).System().Build().Describe().Scope; got != ScopeSystem {
		t.Fatalf("System() scope = %q, want %q", got, ScopeSystem)
	}
	if got := New("profile", handler).WithProfile(InternalEventProfile("runner")).Build().Describe().Scope; got != ScopeInternal {
		t.Fatalf("InternalEventProfile scope = %q, want %q", got, ScopeInternal)
	}
}
