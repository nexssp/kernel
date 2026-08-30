package action

import "time"

// Profile is a named bundle of action metadata and middleware defaults. It
// deliberately excludes a route and description: those are action-specific
// contract details and should remain visible at registration sites.
type Profile struct {
	Tags                []string
	Scope               ActionScope
	Timeout             time.Duration
	ConcurrencyLimit    int32
	SuccessStatus       int
	RequireAuth         bool
	RequiredPermissions []string
	Idempotency         *IdempotencyConfig
}

// WithProfile applies a policy bundle before optional action-specific
// overrides. The returned builder remains fully fluent, so exceptional actions
// can explicitly refine timeout, route, or idempotency configuration.
func (b *Builder[Req, Res]) WithProfile(profile Profile) *Builder[Req, Res] {
	if len(profile.Tags) > 0 {
		b.Tag(profile.Tags...)
	}
	switch profile.Scope {
	case ScopePublic:
		b.Public()
	case ScopeInternal:
		b.Internal()
	case ScopeSystem:
		b.System()
	}
	if profile.RequireAuth && len(profile.RequiredPermissions) == 0 {
		b.RequireAuth()
	}
	for _, permission := range profile.RequiredPermissions {
		b.RequirePermission(permission)
	}
	if profile.Idempotency != nil {
		b.IdempotentWithConfig(*profile.Idempotency)
	}
	if profile.Timeout > 0 {
		b.Timeout(profile.Timeout)
	}
	if profile.ConcurrencyLimit > 0 {
		b.ConcurrencyLimit(profile.ConcurrencyLimit)
	}
	if profile.SuccessStatus > 0 {
		b.SuccessStatus(profile.SuccessStatus)
	}
	return b
}

// AuthenticatedReadProfile is the conservative default for an authenticated,
// read-only query. It intentionally does not enable idempotency because no
// side effect exists to replay.
func AuthenticatedReadProfile(permission string, tags ...string) Profile {
	return Profile{Tags: tags, Scope: ScopePublic, Timeout: 3 * time.Second, RequiredPermissions: []string{permission}}
}

// IdempotentCommandProfile is the default for a mutation that can safely
// replay the same request key. Business code remains responsible for durable
// transactions and external-side-effect coordination.
func IdempotentCommandProfile(permission string, tags ...string) Profile {
	config := IdempotencyConfig{Enabled: true}
	return Profile{Tags: tags, Scope: ScopePublic, Timeout: 3 * time.Second, RequiredPermissions: []string{permission}, Idempotency: &config}
}

// InternalEventProfile is for service-to-service ingestion routes. It requires
// an authenticated transport identity and idempotency, but makes no claim
// about the application-specific identity verifier installed by the service.
func InternalEventProfile(tags ...string) Profile {
	config := IdempotencyConfig{Enabled: true}
	return Profile{Tags: tags, Scope: ScopeInternal, Timeout: 3 * time.Second, RequireAuth: true, Idempotency: &config}
}
