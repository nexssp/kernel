package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type sdkError struct {
	Code int
}

func (e *sdkError) Error() string {
	return fmt.Sprintf("sdk error: %d", e.Code)
}

func main() {
	ctx := context.Background()

	// Safe default: retries only transient xerr errors
	var safeCalls atomic.Int32
	safeAct := action.New("policy.safe", func(ctx context.Context, req string) (string, error) {
		if safeCalls.Add(1) == 1 {
			return "", xerr.Unavailable("temporary")
		}
		return "safe-ok", nil
	}).
		Retry(2, action.ConstantBackoff(time.Millisecond)).
		Build()

	// Custom predicate: retry 5xx SDK errors only
	var sdkCalls atomic.Int32
	sdkAct := action.New("policy.sdk", func(ctx context.Context, req string) (string, error) {
		if sdkCalls.Add(1) == 1 {
			return "", errors.New("just a test")
		}
		return "sdk-ok", nil
	}).
		RetryIf(
			2,
			action.ConstantBackoff(time.Millisecond),
			func(err error) bool {
				var se *sdkError
				if errors.As(err, &se) {
					return se.Code >= 500
				}
				return false
			},
		).
		Build()

	// Retry all errors: good for idempotent scripts
	var idempotentCalls atomic.Int32
	idempotentAct := action.New("policy.retry_all", func(ctx context.Context, req string) (string, error) {
		if idempotentCalls.Add(1) == 1 {
			return "", errors.New("plain error")
		}
		return "idempotent-ok", nil
	}).
		RetryAll(2, action.ConstantBackoff(time.Millisecond)).
		Build()

	// Simulate calls
	_, _ = safeAct.Do(ctx, "safe")
	_, _ = sdkAct.Do(ctx, "sdk")
	_, _ = idempotentAct.Do(ctx, "all")

	fmt.Println("safe calls:", safeCalls.Load())
	fmt.Println("sdk calls:", sdkCalls.Load())
	fmt.Println("idempotent calls:", idempotentCalls.Load())
}
