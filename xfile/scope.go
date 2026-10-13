package xfile

import (
	"context"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// Scope is the authenticated caller and tenant associated with an operation.
// Its values are copied from xctx so they remain stable if the request scope
// is later released to the scope pool.
type Scope struct {
	CallerID string
	TenantID string
}

// ScopeFromContext extracts the authenticated caller and tenant from the
// request scope. Resolvers should call this before authorizing an open and
// must never infer either value from a Ref.
//
// Missing caller or tenant scope is classified as Unauthorized, consistent
// with Kernel's request authorization guards.
func ScopeFromContext(ctx context.Context) (Scope, error) {
	callerID := xctx.UserIDFrom(ctx)
	if callerID == "" {
		return Scope{}, xerr.Unauthorized("authentication required")
	}

	tenantID := xctx.TenantIDFrom(ctx)
	if tenantID == "" {
		return Scope{}, xerr.Unauthorized("tenant context required")
	}

	return Scope{CallerID: callerID, TenantID: tenantID}, nil
}
