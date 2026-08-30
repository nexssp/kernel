package action

import (
	"context"
	"fmt"
)

// AuditLogger defines the minimal contract required by the builder to log audit events.
// This prevents circular import dependencies between action and audit adapters.
type AuditLogger interface {
	Log(ctx context.Context, category string, actionName string, details string)
}

// Audited attaches a generic audit hook invoked after successful action execution.
func (b *Builder[Req, Res]) Audited(logger AuditLogger, category string, detailsFn func(req Req, res Res) string) *Builder[Req, Res] {
	return b.HookExecuted(func(ctx context.Context, req Req, res Res, meta *Meta) {
		if logger == nil {
			return
		}
		details := ""
		if detailsFn != nil {
			details = detailsFn(req, res)
		} else {
			details = fmt.Sprintf("Action %s executed successfully", meta.Name)
		}

		logger.Log(ctx, category, meta.Name, details)
	})
}
