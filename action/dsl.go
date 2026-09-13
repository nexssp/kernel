// path: nexssp/kernel/action/dsl.go
package action

import "time"

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
