package action

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/sync/singleflight"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// Journal persists step results so a resumed execution can replay them
// without re-running the underlying handler. Implementations must be
// safe for concurrent use.
type Journal interface {
	GetStep(ctx context.Context, executionID, stepName string) (data []byte, ok bool, err error)
	RecordStep(ctx context.Context, executionID, stepName string, payload []byte) error
}

// Durable memoizes one step per (executionID, stepName). Concurrent
// callers with the same key share a single underlying invocation; the
// first result is persisted and returned to every waiter.
//
// Without an execution ID in ctx the middleware is a passthrough —
// there is nothing to key the memoization on. The caller is responsible
// for using unique step names within a single execution; two different
// steps sharing a name under the same execution ID will collide.
func Durable[Req, Res any](journal Journal, stepName string) Middleware[Req, Res] {
	var inflight singleflight.Group

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			var zero Res

			if journal == nil || stepName == "" {
				return next(ctx, req)
			}

			executionID := xctx.ExecutionIDFrom(ctx)
			if executionID == "" {
				return next(ctx, req)
			}

			// The shared execution must outlive any single caller's ctx:
			// cancellation of one waiter must not abort work the others
			// are still waiting for.
			sharedCtx := context.WithoutCancel(ctx)
			key := executionID + "\x00" + stepName

			ch := inflight.DoChan(key, func() (any, error) {
				return runDurableStep(sharedCtx, journal, executionID, stepName, next, req)
			})

			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case result := <-ch:
				if result.Err != nil {
					return zero, result.Err
				}
				res, ok := result.Val.(Res)
				if !ok {
					return zero, xerr.Internal(fmt.Sprintf(
						"journal step %q: unexpected result type %T", stepName, result.Val))
				}
				return res, nil
			}
		}
	}
}

// runDurableStep executes the memoization protocol for exactly one
// (executionID, stepName) pair. Called at most once per in-flight key
// by Durable.
func runDurableStep[Req, Res any](
	ctx context.Context,
	journal Journal,
	executionID, stepName string,
	next Fn[Req, Res],
	req Req,
) (Res, error) {
	var zero Res

	recorded, found, err := journal.GetStep(ctx, executionID, stepName)
	if err != nil {
		return zero, xerr.Unavailable(
			fmt.Sprintf("journal read failed for step %q", stepName), err)
	}

	if found && len(recorded) > 0 {
		var memoized Res
		if unmarshalErr := json.Unmarshal(recorded, &memoized); unmarshalErr == nil {
			return memoized, nil
		}
		// Fall through: a recorded payload that no longer decodes into
		// the current Res shape is stale (Res changed since the record
		// was written) and must be replaced.
	}

	result, err := next(ctx, req)
	if err != nil {
		return zero, err
	}

	serialized, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return zero, xerr.Internal(
			fmt.Sprintf("journal marshal failed for step %q", stepName), marshalErr)
	}

	if recordErr := journal.RecordStep(ctx, executionID, stepName, serialized); recordErr != nil {
		return zero, xerr.Unavailable(
			fmt.Sprintf("journal write failed for step %q", stepName), recordErr)
	}

	return result, nil
}

// Durable wires the memoization middleware into a builder.
func (b *Builder[Req, Res]) Durable(journal Journal, stepName string) *Builder[Req, Res] {
	return b.Use(Durable[Req, Res](journal, stepName))
}
