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
	safeAct := action.New("policy.safe", func(_ context.Context, _ string) (string, error) {
		if safeCalls.Add(1) == 1 {
			return "", xerr.Unavailable("temporary")
		}
		return "safe-ok", nil
	}).
		Retry(2, action.ConstantBackoff(time.Millisecond)).
		Build()

	var sdkCalls atomic.Int32
	sdkAct := action.New("policy.sdk", func(_ context.Context, _ string) (string, error) {
		if sdkCalls.Add(1) == 1 {
			return "", errors.New("just a test")
		}
		return "sdk-ok", nil
	}).
		RetryIf(
			2,
			action.ConstantBackoff(time.Millisecond),
			func(err error) bool {
				if se, ok := errors.AsType[*sdkError](err); ok {
					return se.Code >= 500
				}
				return false
			},
		).
		Build()

	var idempotentCalls atomic.Int32
	idempotentAct := action.New("policy.retry_all", func(_ context.Context, _ string) (string, error) {
		if idempotentCalls.Add(1) == 1 {
			return "", errors.New("plain error")
		}
		return "idempotent-ok", nil
	}).
		RetryAll(2, action.ConstantBackoff(time.Millisecond)).
		Build()

	if _, err := safeAct.Do(ctx, "safe"); err != nil {
		fmt.Printf("safeAct error: %v\n", err)
	}
	if _, err := sdkAct.Do(ctx, "sdk"); err != nil {
		fmt.Printf("sdkAct error: %v\n", err)
	}
	if _, err := idempotentAct.Do(ctx, "all"); err != nil {
		fmt.Printf("idempotentAct error: %v\n", err)
	}

	fmt.Println("safe calls:", safeCalls.Load())
	fmt.Println("sdk calls:", sdkCalls.Load())
	fmt.Println("idempotent calls:", idempotentCalls.Load())
}
