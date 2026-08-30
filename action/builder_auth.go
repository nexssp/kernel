package action

// builder_auth.go — RBAC and feature-flag guards as fluent builder methods.
//
// Because these are methods on Builder[Req, Res], Go infers both type
// parameters from the receiver. Callers never write type annotations:
//
//	// Before (manual middleware):
//	action.New("order.delete", handler).
//	    Use(middleware.RequireRole[DeleteReq, DeleteRes]("admin")).
//	    Use(middleware.RequirePermission[DeleteReq, DeleteRes]("orders:delete")).
//	    Build()
//
//	// After (fluent, zero type annotation):
//	action.New("order.delete", handler).
//	    RequireRole("admin").
//	    RequirePermission("orders:delete").
//	    Build()

import (
	"context"
	"strings"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// RequireRole aborts with 403 Forbidden if ctx does not contain the given role.
// Roles are injected by the JWT middleware from the "roles" claim.
//
//	deleteOrderAct := action.New("order.delete", handler).
//	    RequireRole("admin").
//	    Route(thttp.DELETE("/api/v1/orders/{id}")).
//	    Build()
func (b *Builder[Req, Res]) RequireRole(role string) *Builder[Req, Res] {
	b.meta.RequiredRoles = append(b.meta.RequiredRoles, role)
	b.meta.RequiresAuth = true
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !xctx.HasRole(ctx, role) {
				var zero Res
				return zero, xerr.Forbidden("requires role: " + role)
			}
			return next(ctx, req)
		}
	})
}

// RequireAnyRole aborts with 403 Forbidden if ctx contains none of the given roles.
// First matching role passes — order does not matter.
//
//	approveRefundAct := action.New("refund.approve", handler).
//	    RequireAnyRole("admin", "finance", "support").
//	    Build()
func (b *Builder[Req, Res]) RequireAnyRole(roles ...string) *Builder[Req, Res] {
	b.meta.RequiredRoles = append(b.meta.RequiredRoles, roles...)
	b.meta.RequiresAuth = true
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !xctx.HasAnyRole(ctx, roles...) {
				var zero Res
				return zero, xerr.Forbidden("requires one of roles: " + strings.Join(roles, ", "))
			}
			return next(ctx, req)
		}
	})
}

// RequirePermission aborts with 403 Forbidden if ctx does not contain
// the given fine-grained permission string.
// Permissions are injected from the "perms" JWT claim.
//
//	exportAct := action.New("data.export", handler).
//	    RequirePermission("data:export").
//	    Build()
func (b *Builder[Req, Res]) RequirePermission(perm string) *Builder[Req, Res] {
	b.meta.RequiredPermissions = append(b.meta.RequiredPermissions, perm)
	b.meta.RequiresAuth = true
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if !xctx.HasPermission(ctx, perm) {
				var zero Res
				return zero, xerr.Forbidden("requires permission: " + perm)
			}
			return next(ctx, req)
		}
	})
}

// RequireFeature aborts with 403 Forbidden if ALL listed feature flags
// are not enabled in ctx (logical AND — every flag must be on).
// Feature flags are injected from the "features" JWT claim.
//
// Single flag:
//
//	aiCheckoutAct := action.New("checkout.ai", handler).
//	    RequireFeature("ai-checkout").
//	    Build()
//
// Multiple flags (all must be enabled):
//
//	premiumExportAct := action.New("export.premium", handler).
//	    RequireFeature("premium-plan", "data-export").
//	    Build()
func (b *Builder[Req, Res]) RequireFeature(keys ...string) *Builder[Req, Res] {
	b.meta.RequiredFeatures = append(b.meta.RequiredFeatures, keys...)
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			for _, k := range keys {
				if !xctx.HasFeature(ctx, k) {
					var zero Res
					return zero, xerr.Forbidden("feature not enabled: " + k)
				}
			}
			return next(ctx, req)
		}
	})
}

// RequireAnyFeature aborts with 403 Forbidden if NONE of the listed
// feature flags are enabled in ctx (logical OR — at least one must be on).
//
//	betaOrPremiumAct := action.New("widget.beta", handler).
//	    RequireAnyFeature("beta-access", "premium-plan").
//	    Build()
func (b *Builder[Req, Res]) RequireAnyFeature(keys ...string) *Builder[Req, Res] {
	b.meta.RequiredFeatures = append(b.meta.RequiredFeatures, keys...)
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			for _, k := range keys {
				if xctx.HasFeature(ctx, k) {
					return next(ctx, req)
				}
			}
			var zero Res
			return zero, xerr.Forbidden("requires one of features: " + strings.Join(keys, ", "))
		}
	})
}

// RequireAuth aborts with 401 Unauthorized if ctx has no authenticated user
// (UserID is empty). Use when an endpoint requires login but any role is fine.
//
//	myProfileAct := action.New("user.me", handler).
//	    RequireAuth().
//	    Build()
func (b *Builder[Req, Res]) RequireAuth() *Builder[Req, Res] {
	b.meta.RequiresAuth = true
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if xctx.UserIDFrom(ctx) == "" {
				var zero Res
				return zero, xerr.Unauthorized("authentication required")
			}
			return next(ctx, req)
		}
	})
}

// RequireTenant aborts with 401 Unauthorized if ctx has no tenant ID.
// Use to guard multi-tenant endpoints from unauthenticated callers.
//
//	tenantOrderAct := action.New("order.list", handler).
//	    RequireTenant().
//	    Build()
func (b *Builder[Req, Res]) RequireTenant() *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			if xctx.TenantIDFrom(ctx) == "" {
				var zero Res
				return zero, xerr.Unauthorized("tenant context required")
			}
			return next(ctx, req)
		}
	})
}
