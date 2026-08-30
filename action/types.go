package action

import (
	"context"
	"strings"
	"time"
)

type Plugin interface {
	HookProvider
	ActionProvider
}

type HookProvider interface {
	GetAnyHooks() []AnyHook
}

// ActionProvider exposes the registered actions of an application.
type ActionProvider interface {
	Actions() []AnyAction
}

// Bootstrapper allows plugins/actions to hook into the boot lifecycle.
type Bootstrapper interface {
	OnBoot(app ActionProvider) error
}

type Fn[Req, Res any] func(context.Context, Req) (Res, error)

type Middleware[Req, Res any] func(Fn[Req, Res]) Fn[Req, Res]

type DispatcherMiddleware[Req, Res any] func(next Fn[Req, Res], hooks HookDispatcher[Req, Res]) Fn[Req, Res]

type HookDispatcher[Req, Res any] interface {
	OnCacheHit(ctx context.Context, req Req, res Res)
	OnCacheMiss(ctx context.Context, req Req)
	OnRetry(ctx context.Context, req Req, attempt int, err error)
	OnCoalesced(ctx context.Context, req Req)
	OnDeduplicated(ctx context.Context, req Req)
}

type Binding any

type Hook[Req, Res any] struct {
	OnRegister     func(meta *Meta)
	Before         func(ctx context.Context, req Req, meta *Meta) (context.Context, error)
	After          func(ctx context.Context, req Req, res Res, err error, meta *Meta)
	OnError        func(ctx context.Context, req Req, err error, meta *Meta)
	OnRetry        func(ctx context.Context, req Req, attempt int, err error, meta *Meta)
	OnCacheHit     func(ctx context.Context, req Req, res Res, meta *Meta)
	OnCacheMiss    func(ctx context.Context, req Req, meta *Meta)
	OnCoalesced    func(ctx context.Context, req Req, meta *Meta)
	OnDeduplicated func(ctx context.Context, req Req, meta *Meta)
	OnCancel       func(ctx context.Context, req Req, meta *Meta)
	OnExecuted     func(ctx context.Context, req Req, res Res, err error, meta *Meta)
}

// AnyHook is used for broad plugins (metrics, tracing, auth).
// Passing 'meta' dynamically guarantees thread-safety and zero allocations across shared instances.
type AnyHook struct {
	OnRegister     func(meta *Meta)
	Before         func(ctx context.Context, req any, meta *Meta) (context.Context, error)
	After          func(ctx context.Context, req any, res any, err error, meta *Meta)
	OnError        func(ctx context.Context, req any, err error, meta *Meta)
	OnRetry        func(ctx context.Context, req any, attempt int, err error, meta *Meta)
	OnCacheHit     func(ctx context.Context, req any, res any, meta *Meta)
	OnCacheMiss    func(ctx context.Context, req any, meta *Meta)
	OnCoalesced    func(ctx context.Context, req any, meta *Meta)
	OnDeduplicated func(ctx context.Context, req any, meta *Meta)
	OnCancel       func(ctx context.Context, req any, meta *Meta)
	OnExecuted     func(ctx context.Context, req any, res any, err error, meta *Meta)
	OnPanic        func(ctx context.Context, req any, recovered any, meta *Meta)
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
type ActionScope string

const (
	// ScopePublic is the browser/client business contract. It is the zero-value
	// behavior so normal client actions remain concise.
	ScopePublic ActionScope = "public"
	// ScopeInternal is a trusted service-to-service or runner contract. It is
	// included only in explicitly requested trusted SDK generation.
	ScopeInternal ActionScope = "internal"
	// ScopeSystem is a framework or operational contract. It is excluded from
	// public and trusted business contracts.
	ScopeSystem ActionScope = "system"
)

// IsSystem reports whether the action belongs to the framework/operations plane.
func (m *Meta) IsSystem() bool { return m != nil && m.Scope == ScopeSystem }

// IsInternal reports whether the action belongs only to trusted callers.
func (m *Meta) IsInternal() bool { return m != nil && m.Scope == ScopeInternal }

// IsPublic reports whether the action is part of the public business contract.
// The zero value is public for concise ordinary client actions.
func (m *Meta) IsPublic() bool { return m != nil && (m.Scope == "" || m.Scope == ScopePublic) }

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

// DecodeFunc allows transports to inject data directly into the concrete type.
type DecodeFunc func(v any) error

// Executable handles the HOT path — execution only.
type Executable interface {
	ExecuteDecoded(ctx context.Context, decode DecodeFunc) (any, error)
}

// Describable handles the COLD path — boot, discovery, routing, and CLI help.
type Describable interface {
	Describe() *Meta
}

// AnyAction is the type-erased interface for the App and Transports to handle actions.
type AnyAction interface {
	Executable
	Describable
	GetBindings() []Binding
	GetAnyHooks() []AnyHook
	AddAnyHook(h ...AnyHook)
}

// TypedPayload allows plugins (like OpenAPI) to discover the underlying Request and Response
// types at boot time without storing them in metadata or using reflection during execution.
type TypedPayload interface {
	ReqPayload() any
	ResPayload() any
}

// MessageRes is a standard DTO for actions that only need to return a text message.
// Using a strongly-typed struct instead of map[string]string ensures precise SDK generation
// and clean OpenAPI documentation.
type MessageRes struct {
	Message string `json:"message"`
}

// assertTo converts any to T, falling back to the zero value when the type
// assertion fails. Adapt relies on this so hooks stay resilient to nil or
// mismatched payloads instead of panicking.
func assertTo[T any](v any) T {
	if t, ok := v.(T); ok {
		return t
	}
	var zero T
	return zero
}

// Adapt converts a type-safe Hook[Req, Res] into the standardized AnyHook container.
// It uses type assertions with safe zero-value fallbacks so hooks fire reliably even when res is nil.
func Adapt[Req, Res any](h Hook[Req, Res]) AnyHook {
	return AnyHook{
		OnRegister: h.OnRegister,
		Before: func(ctx context.Context, req any, meta *Meta) (context.Context, error) {
			if h.Before == nil {
				return ctx, nil
			}
			return h.Before(ctx, assertTo[Req](req), meta)
		},
		After: func(ctx context.Context, req any, res any, err error, meta *Meta) {
			if h.After == nil {
				return
			}
			h.After(ctx, assertTo[Req](req), assertTo[Res](res), err, meta)
		},
		OnError: func(ctx context.Context, req any, err error, meta *Meta) {
			if h.OnError == nil {
				return
			}
			h.OnError(ctx, assertTo[Req](req), err, meta)
		},
		OnExecuted: func(ctx context.Context, req any, res any, err error, meta *Meta) {
			if h.OnExecuted == nil || err != nil {
				return
			}
			h.OnExecuted(ctx, assertTo[Req](req), assertTo[Res](res), err, meta)
		},
		OnCacheHit: func(ctx context.Context, req any, res any, meta *Meta) {
			if h.OnCacheHit == nil {
				return
			}
			h.OnCacheHit(ctx, assertTo[Req](req), assertTo[Res](res), meta)
		},
		OnCacheMiss: func(ctx context.Context, req any, meta *Meta) {
			if h.OnCacheMiss == nil {
				return
			}
			h.OnCacheMiss(ctx, assertTo[Req](req), meta)
		},
		OnRetry: func(ctx context.Context, req any, attempt int, err error, meta *Meta) {
			if h.OnRetry == nil {
				return
			}
			h.OnRetry(ctx, assertTo[Req](req), attempt, err, meta)
		},
		OnCoalesced: func(ctx context.Context, req any, meta *Meta) {
			if h.OnCoalesced == nil {
				return
			}
			h.OnCoalesced(ctx, assertTo[Req](req), meta)
		},
		OnDeduplicated: func(ctx context.Context, req any, meta *Meta) {
			if h.OnDeduplicated == nil {
				return
			}
			h.OnDeduplicated(ctx, assertTo[Req](req), meta)
		},
		OnCancel: func(ctx context.Context, req any, meta *Meta) {
			if h.OnCancel == nil {
				return
			}
			h.OnCancel(ctx, assertTo[Req](req), meta)
		},
	}
}
