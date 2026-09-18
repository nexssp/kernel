// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

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
		details := fmt.Sprintf("Action %s executed successfully", meta.Name)
		if detailsFn != nil {
			details = detailsFn(req, res)
		}

		logger.Log(ctx, category, meta.Name, details)
	})
}

// PIITracker defines the minimal contract for personal data access tracking.
type PIITracker interface {
	TrackPIIAccess(ctx context.Context, purpose string, actionName string)
}

// TrackPIIAccess attaches a non-blocking hook recording the purpose of PII data access.
func (b *Builder[Req, Res]) TrackPIIAccess(tracker PIITracker, purpose string) *Builder[Req, Res] {
	return b.HookExecuted(func(ctx context.Context, _ Req, _ Res, meta *Meta) {
		if tracker == nil {
			return
		}
		tracker.TrackPIIAccess(ctx, purpose, meta.Name)
	})
}
