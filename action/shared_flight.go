// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"time"
)

// DefaultSharedFlightTimeout bounds the context passed to coalesced or deduplicated work.
const DefaultSharedFlightTimeout = 2 * time.Minute

// SharedFlightOptions configures the deadline for coalesced or deduplicated work.
// A non-positive Timeout selects DefaultSharedFlightTimeout. Shared work keeps
// the leader's context values but ignores caller cancellation. It runs
// synchronously; handlers that ignore the shared timeout may delay the caller.
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
	if _, hasDeadline := ctx.Deadline(); hasDeadline || ctx.Done() != nil {
		ctx = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
