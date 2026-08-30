package action

import (
	"context"
)

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
