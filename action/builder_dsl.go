// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

// WithDSL applies every non-zero field of m to the builder. Designed for the
// declarative overlay pattern: parse a .flow / YAML manifest once at boot,
// then for each registered action call WithDSL with its matching modifiers.
//
// Ordering matters — scope is applied first so later auth/permission wrappers
// see the correct default scope; hooks are applied last so they wrap the
// fully configured action.
func (b *Builder[Req, Res]) WithDSL(m DSLModifiers) *Builder[Req, Res] {
	b = b.applyScope(m.Scope)
	b = b.applyAuth(m)
	b = b.applyResilience(m)
	b = b.applyCache(m)
	b = b.applyMetadata(m)
	return b
}

func (b *Builder[Req, Res]) applyScope(scope string) *Builder[Req, Res] {
	switch scope {
	case "public":
		return b.Public()
	case "internal":
		return b.Internal()
	case "system":
		return b.System()
	}
	return b
}

func (b *Builder[Req, Res]) applyAuth(m DSLModifiers) *Builder[Req, Res] {
	if m.RequiresAuth {
		b = b.RequireAuth()
	}
	for _, r := range m.Roles {
		b = b.RequireRole(r)
	}
	for _, p := range m.Permissions {
		b = b.RequirePermission(p)
	}
	for _, f := range m.Features {
		b = b.RequireFeature(f)
	}
	return b
}

func (b *Builder[Req, Res]) applyResilience(m DSLModifiers) *Builder[Req, Res] {
	if m.RateLimit > 0 {
		b = b.RateLimit(m.RateLimit, m.Burst)
	}
	if m.Concurrency > 0 {
		b = b.ConcurrencyLimit(m.Concurrency)
	}
	if m.Timeout > 0 {
		b = b.Timeout(m.Timeout)
	}
	if m.RetryMax > 0 {
		backoff := m.RetryBackoff
		if backoff == nil {
			backoff = ExponentialJitter(100*time.Millisecond, 30*time.Second)
		}
		if m.RetryIf != nil {
			b = b.RetryIf(m.RetryMax, backoff, m.RetryIf)
		} else {
			b = b.Retry(m.RetryMax, backoff)
		}
	}
	if m.Idempotent {
		if m.Idempotency != nil {
			b = b.IdempotentWithConfig(*m.Idempotency)
		} else {
			b = b.Idempotent()
		}
	}
	return b
}

func (b *Builder[Req, Res]) applyCache(m DSLModifiers) *Builder[Req, Res] {
	if m.CacheTTL <= 0 {
		return b
	}
	keyFn := defaultCacheKeyFn[Req]
	if m.CacheKeyFnRaw != nil {
		if fn, ok := m.CacheKeyFnRaw.(func(Req) string); ok {
			keyFn = fn
		}
	}
	return b.Cache(m.CacheTTL, keyFn)
}

func (b *Builder[Req, Res]) applyMetadata(m DSLModifiers) *Builder[Req, Res] {
	for _, t := range m.Tags {
		b = b.Tag(t)
	}
	if m.SuccessStatus > 0 {
		b = b.SuccessStatus(m.SuccessStatus)
	}
	if len(m.Hooks) > 0 {
		b = b.AnyHook(m.Hooks...)
	}
	return b
}

// ApplyDSL bridges the generic builder to the non-generic DSLApplier interface.
func (a *BuiltAction[Req, Res]) ApplyDSL(m DSLModifiers) AnyAction {
	return a.ToBuilder().WithDSL(m).Build()
}

// defaultCacheKeyFn produces a deterministic hash of the request payload.
// Used when a DSL overlay declares :cache=... but no custom key function.
func defaultCacheKeyFn[Req any](req Req) string {
	data, err := json.Marshal(req)
	if err != nil {
		return "invalid"
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
