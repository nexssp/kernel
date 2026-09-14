package action

import (
	"time"

	"github.com/nexssp/kernel/xerr"
)

// SmartResilience automatically applies intelligent backoff and circuit breaking
// based entirely on the xerr.Kind of the returned error.
// It delegates to the universal RetryIf middleware to ensure 100% hook/telemetry fidelity.
func SmartResilience[Req, Res any](name string) Middleware[Req, Res] {
	cb := Adaptive[Req, Res](name, AdaptiveConfig{
		FailureThreshold: 5,
		ResetTimeout:     10 * time.Second,
		InitialTimeout:   3 * time.Second,
	})

	backoff := ExponentialJitter(100*time.Millisecond, 2*time.Second)

	// Smart predicate: retry anything that is NOT a permanent 4xx error
	predicate := func(err error) bool {
		return err != nil && !xerr.IsPermanent(err)
	}

	retry := RetryWithPredicateMiddleware[Req, Res](3, backoff, predicate)

	return func(next Fn[Req, Res]) Fn[Req, Res] {
		// Kolejność ma znaczenie: Retry wewnątrz Circuit Breakera
		// cb(retry(next)) -> błędy z retry uderzają w CB.
		return cb(retry(next, nil))
	}
}

func (b *Builder[Req, Res]) InferredResilient() *Builder[Req, Res] {
	return b.Use(SmartResilience[Req, Res](b.meta.Name))
}
