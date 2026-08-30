package action

import (
	"context"
	"fmt"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

type QuotaCheckFunc func(ctx context.Context, tenantID string) (current int64, limit int64, err error)

// RequireCreationLimit enforces plan quotas ONLY during new entity creation (ID == 0).
// Updates to existing resources bypass this check.
func (b *Builder[Req, Res]) RequireCreationLimit(resourceName string, checkFn QuotaCheckFunc) *Builder[Req, Res] {
	return b.Use(func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			// Fast bypass if this is an update (ID > 0)
			if idGetter, ok := any(req).(interface{ GetID() uint }); ok && idGetter.GetID() > 0 {
				return next(ctx, req)
			}

			tenantID := xctx.TenantIDFrom(ctx)
			if tenantID == "" || tenantID == "local_desktop" {
				return next(ctx, req)
			}

			current, limit, err := checkFn(ctx, tenantID)
			if err != nil {
				var zero Res
				return zero, xerr.Internal("quota check failed: "+resourceName, err)
			}

			if limit != -1 && current >= limit {
				var zero Res
				return zero, xerr.Forbidden(fmt.Sprintf("quota exceeded for %s (%d/%d)", resourceName, current, limit))
			}

			return next(ctx, req)
		}
	})
}
