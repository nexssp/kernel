package action

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// Journal persists step results so a resumed execution can replay them
// without re-running the underlying handler. Implementations must be
// safe for concurrent use. RecordStep errors may be ambiguous: an
// implementation can commit a payload and then return an error, so callers
// must not treat a failed write as proof that no external side effect or
// journal record took effect.
type Journal interface {
	GetStep(ctx context.Context, executionID, stepName string) (data []byte, ok bool, err error)
	RecordStep(ctx context.Context, executionID, stepName string, payload []byte) error
}

type durableResult[Res any] struct {
	value Res
}

const (
	// DefaultDurableMaxConcurrentExecutions caps active unique step executions
	// per Durable middleware when no positive limit is provided.
	DefaultDurableMaxConcurrentExecutions = 16
	// DefaultDurableTimeout sets the default context deadline for one durable flight.
	DefaultDurableTimeout = 2 * time.Minute
)

// DurableOptions bounds unique in-flight durable executions created by one
// middleware. Same-key callers still share one flight. A non-positive field
// selects its default; Timeout covers the semaphore wait, journal I/O, and
// handler call. The timeout is cooperative for implementations that ignore ctx.
type DurableOptions struct {
	MaxConcurrentExecutions int
	Timeout                 time.Duration
}

func (o DurableOptions) withDefaults() DurableOptions {
	if o.MaxConcurrentExecutions <= 0 {
		o.MaxConcurrentExecutions = DefaultDurableMaxConcurrentExecutions
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultDurableTimeout
	}
	return o
}

// Durable memoizes one step per (executionID, stepName). Concurrent
// callers with the same key share a single underlying invocation; the
// first result is persisted and returned to every waiter.
//
// Without an execution ID in ctx the middleware is a passthrough —
// there is nothing to key the memoization on. The caller is responsible
// for using unique step names within a single execution; two different
// steps sharing a name under the same execution ID will collide.
//
// Results are persisted as JSON. For empty-interface Res values such as any,
// both the first successful call and later replays use encoding/json's default
// interface representation (for example, numbers become float64 and typed nil
// pointers become nil). Arbitrary Go dynamic types are not restored from
// untrusted type names.
func Durable[Req, Res any](journal Journal, stepName string) Middleware[Req, Res] {
	return DurableWithOptions[Req, Res](journal, stepName, DurableOptions{})
}

// DurableWithOptions creates a durable middleware with explicit concurrency
// and timeout limits. Zero-value options use conservative defaults.
func DurableWithOptions[Req, Res any](journal Journal, stepName string, options DurableOptions) Middleware[Req, Res] {
	options = options.withDefaults()
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		run := newDurableRunner(journal, stepName, next, options)
		return func(ctx context.Context, req Req) (Res, error) {
			return run(ctx, req, xctx.ExecutionIDFrom(ctx))
		}
	}
}

func newDurableRunner[Req, Res any](journal Journal, stepName string, next Fn[Req, Res], options DurableOptions) func(context.Context, Req, string) (Res, error) {
	options = options.withDefaults()
	var inflight singleflight.Group
	semaphore := make(chan struct{}, options.MaxConcurrentExecutions)

	return func(ctx context.Context, req Req, executionID string) (Res, error) {
		var zero Res
		if journal == nil || stepName == "" || executionID == "" {
			return next(ctx, req)
		}

		sharedCtx := context.WithoutCancel(ctx)
		key := executionID + "\x00" + stepName
		ch := inflight.DoChan(key, func() (any, error) {
			runCtx, cancel := context.WithTimeout(sharedCtx, options.Timeout)
			defer cancel()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-runCtx.Done():
				return nil, runCtx.Err()
			}
			value, err := runDurableStep(runCtx, journal, executionID, stepName, next, req)
			if err != nil {
				return nil, err
			}
			return durableResult[Res]{value: value}, nil
		})

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case result := <-ch:
			if result.Err != nil {
				return zero, result.Err
			}
			memoized, ok := result.Val.(durableResult[Res])
			if !ok {
				return zero, xerr.Internal(fmt.Sprintf(
					"journal step %q: unexpected result type %T", stepName, result.Val))
			}
			return memoized.value, nil
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

	if found {
		if !json.Valid(recorded) {
			var memoized Res
			unmarshalErr := json.Unmarshal(recorded, &memoized)
			return zero, xerr.Internal(
				fmt.Sprintf("journal payload for step %q is invalid JSON", stepName), unmarshalErr)
		}

		var memoized Res
		if unmarshalErr := json.Unmarshal(recorded, &memoized); unmarshalErr != nil {
			return zero, xerr.Internal(
				fmt.Sprintf("journal payload for step %q is incompatible with current result type", stepName), unmarshalErr)
		}
		return memoized, nil
	}

	result, err := next(ctx, req)
	if err != nil {
		return zero, err
	}
	result = normalizeJSONNullResult(result)

	serialized, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return zero, xerr.Internal(
			fmt.Sprintf("journal marshal failed for step %q", stepName), marshalErr)
	}
	result, marshalErr = normalizeJSONReplayResult(result, serialized)
	if marshalErr != nil {
		return zero, xerr.Internal(
			fmt.Sprintf("journal result normalization failed for step %q", stepName), marshalErr)
	}

	if recordErr := journal.RecordStep(ctx, executionID, stepName, serialized); recordErr != nil {
		return zero, xerr.Unavailable(
			fmt.Sprintf("journal write failed for step %q", stepName), recordErr)
	}

	return result, nil
}

func normalizeJSONNullResult[Res any](result Res) Res {
	if reflect.TypeFor[Res]().Kind() != reflect.Interface {
		return result
	}
	value := reflect.ValueOf(result)
	if !value.IsValid() {
		return result
	}
	//nolint:exhaustive // Only nullable kinds can encode a typed nil as JSON null.
	switch value.Kind() {
	case reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			var zero Res
			return zero
		}
	default:
		return result
	}
	return result
}

func normalizeJSONReplayResult[Res any](result Res, payload []byte) (Res, error) {
	resultType := reflect.TypeFor[Res]()
	if resultType.Kind() != reflect.Interface || resultType.NumMethod() != 0 {
		return result, nil
	}
	var normalized any
	if err := json.Unmarshal(payload, &normalized); err != nil {
		var zero Res
		return zero, err
	}
	if normalized == nil {
		var zero Res
		return zero, nil
	}
	if result, ok := normalized.(Res); ok {
		return result, nil
	}
	var zero Res
	return zero, fmt.Errorf("JSON result with dynamic type %T is not assignable to Res", normalized)
}

// Durable wires the memoization middleware into a builder.
func (b *Builder[Req, Res]) Durable(journal Journal, stepName string) *Builder[Req, Res] {
	return b.Use(Durable[Req, Res](journal, stepName))
}

// DurableWithOptions wires a durable middleware with explicit execution limits.
func (b *Builder[Req, Res]) DurableWithOptions(journal Journal, stepName string, options DurableOptions) *Builder[Req, Res] {
	return b.Use(DurableWithOptions[Req, Res](journal, stepName, options))
}
