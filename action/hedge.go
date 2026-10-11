package action

import (
	"context"
	"sync"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// Hedge runs act and launches one more attempt if the first has not finished
// within delay, or has failed with a non-permanent error. First success wins;
// the other attempt is canceled and awaited before returning, so act must
// honor ctx. If every attempt fails, the last error is returned unchanged.
//
// delay <= 0 disables hedging.
func Hedge[Req, Res any](name string, act *BuiltAction[Req, Res], delay time.Duration) *Builder[Req, Res] {
	return New(name, func(ctx context.Context, req Req) (Res, error) {
		var zero Res
		if delay <= 0 {
			return act.Do(ctx, req)
		}

		runCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		defer func() {
			cancel()
			wg.Wait()
		}()

		type outcome struct {
			val Res
			err error
		}
		outcomes := make(chan outcome, 2)
		launched := 0
		launch := func() {
			launched++
			wg.Go(func() {
				val, err := act.Do(runCtx, req)
				outcomes <- outcome{val, err}
			})
		}

		launch()
		pending := 1
		timer := time.NewTimer(delay)
		defer timer.Stop()

		var lastErr error
		for pending > 0 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-timer.C:
				if launched < 2 {
					launch()
					pending++
				}
			case out := <-outcomes:
				pending--
				if out.err == nil {
					return out.val, nil
				}
				lastErr = out.err
				if xerr.IsPermanent(out.err) {
					return zero, out.err
				}
				if launched < 2 {
					launch()
					pending++
				}
			}
		}
		return zero, lastErr
	}).
		Description("Hedged execution: second attempt after delay or on transient failure").
		Tag("resilience", "latency")
}
