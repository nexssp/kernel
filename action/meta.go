// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"strings"
	"time"
)

// DSLModifiers is the runtime subset of declarative metadata that a
// declarative overlay (a .flow file, a YAML manifest, etc.) can apply to a
// BuiltAction after construction. Fields map 1:1 to Builder methods that
// already exist; the struct exists only to bridge generic-typed Builders
// with non-generic action registries.
//
// Zero value is "apply nothing" — leave every field at its default to skip.
type DSLModifiers struct {
	// Scope — "public" | "internal" | "system". Empty = leave unchanged.
	Scope string

	// Auth
	RequiresAuth bool
	Roles        []string
	Permissions  []string
	Features     []string

	// Resilience
	RateLimit     float64
	Burst         int
	Concurrency   int32
	Timeout       time.Duration
	RetryMax      int
	RetryBackoff  func(attempt int) time.Duration
	RetryIf       RetryPredicate
	Idempotent    bool
	Idempotency   *IdempotencyConfig
	CacheTTL      time.Duration
	CacheKeyFnRaw any // func(Req) string — type asserted inside WithDSL

	// Metadata
	Tags          []string
	SuccessStatus int

	// Extra hooks (HITL, budget guard, audit, custom middleware).
	// Passed straight through to Builder.AnyHook.
	Hooks []AnyHook
}

// DSLApplier is implemented by every BuiltAction. It lets a non-generic
// registry ([]AnyAction) delegate modifier application to the generic
// builder machinery without knowing Req/Res.
type DSLApplier interface {
	ApplyDSL(m DSLModifiers) AnyAction
}

var _ DSLApplier = (*BuiltAction[struct{}, struct{}])(nil)

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

type Meta struct {
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	Node             string            `json:"node,omitempty"`
	Tags             []string          `json:"tags,omitempty"`
	Scope            ActionScope       `json:"scope,omitempty"`
	Idempotency      IdempotencyConfig `json:"idempotency"`
	SuccessStatus    int               `json:"success_status,omitempty"`
	LogSlowThreshold time.Duration     `json:"log_slow_threshold,omitempty"`
	Example          any               `json:"example,omitempty"`

	RequiredRoles       []string `json:"required_roles,omitempty"`
	RequiredPermissions []string `json:"required_permissions,omitempty"`
	RequiredFeatures    []string `json:"required_features,omitempty"`
	RequiresAuth        bool     `json:"requires_auth,omitempty"`

	RetryMax         int           `json:"retry_max,omitempty"`
	Timeout          time.Duration `json:"timeout,omitempty"`
	ConcurrencyLimit int32         `json:"concurrency_limit,omitempty"`
	RateLimit        string        `json:"rate_limit,omitempty"`
	CacheTTL         time.Duration `json:"cache_ttl,omitempty"`
	Deduplicated     bool          `json:"deduplicated,omitempty"`
	Coalesced        bool          `json:"coalesced,omitempty"`
}

// ActionScope describes the contract audience for an action. It is not an
// authorization rule: authentication and authorization remain enforced by the
// action's transport middleware and guards.
//
//nolint:revive // exported name kept for API stability
type ActionScope string

const (
	// ScopePublic is the browser/client business contract. It is the zero-value
	// behavior so normal client actions remain concise.
	ScopePublic ActionScope = ""
	// ScopeInternal is a trusted service-to-service or runner contract.
	ScopeInternal ActionScope = "internal"
	// ScopeSystem is a framework or operational contract.
	ScopeSystem ActionScope = "system"
)

// IsSystem reports whether the action belongs to the framework/operations plane.
func (m *Meta) IsSystem() bool { return m != nil && m.Scope == ScopeSystem }

// IsInternal reports whether the action belongs only to trusted callers.
func (m *Meta) IsInternal() bool { return m != nil && m.Scope == ScopeInternal }

// IsPublic reports whether the action is part of the public business contract.
// The zero value is public for concise ordinary client actions.
func (m *Meta) IsPublic() bool { return m != nil && m.Scope == ScopePublic }

func (a *BuiltAction[Req, Res]) String() string {
	return a.meta.String()
}

func (m Meta) String() string {
	s := m.Name
	if m.Description != "" {
		s += ": " + m.Description
	}
	if len(m.Tags) > 0 {
		s += " [" + strings.Join(m.Tags, ",") + "]"
	}
	return s
}

// MessageRes is a standard DTO for actions that only need to return a text message.
// Using a strongly-typed struct instead of map[string]string ensures precise SDK generation
// and clean OpenAPI documentation.
type MessageRes struct {
	Message string `json:"message"`
}

type NodeFilterPolicy struct {
	ActiveNode              string
	DefaultNode             string
	AllowUntaggedEverywhere bool
}

func (m *Meta) MatchesNode(policy NodeFilterPolicy) bool {
	if m == nil {
		return false
	}

	// Monolith mode: every action is local.
	if policy.ActiveNode == "" {
		return true
	}

	// System actions must run everywhere: /health, /metrics, etc.
	if m.IsSystem() {
		return true
	}

	// Exact node placement.
	if m.Node == policy.ActiveNode {
		return true
	}

	// Untagged actions.
	if m.Node == "" {
		if policy.AllowUntaggedEverywhere {
			return true
		}
		if policy.DefaultNode != "" && policy.ActiveNode == policy.DefaultNode {
			return true
		}
	}

	return false
}

func FilterByNode(actions []AnyAction, policy NodeFilterPolicy) []AnyAction {
	if policy.ActiveNode == "" {
		return actions
	}
	filtered := make([]AnyAction, 0, len(actions))
	for _, act := range actions {
		if act != nil && act.Describe() != nil && act.Describe().MatchesNode(policy) {
			filtered = append(filtered, act)
		}
	}
	return filtered
}
