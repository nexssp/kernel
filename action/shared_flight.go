// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"time"
)

// DefaultSharedFlightTimeout bounds the context passed to a coalesce or deduplication leader.
const DefaultSharedFlightTimeout = 2 * time.Minute

// SharedFlightOptions configures the context deadline for a coalesced or deduplicated leader.
// A non-positive Timeout selects DefaultSharedFlightTimeout. The handler still runs synchronously;
// handlers that ignore context cancellation cannot be forcibly stopped and may delay the caller.
type SharedFlightOptions struct {
	// Timeout bounds the leader context; non-positive values use the default.
	Timeout time.Duration
}

func (options SharedFlightOptions) withDefaults() SharedFlightOptions {
	if options.Timeout <= 0 {
		options.Timeout = DefaultSharedFlightTimeout
	}
	return options
}

func withSharedFlightTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, nil
	}
	return context.WithTimeout(ctx, timeout)
}
