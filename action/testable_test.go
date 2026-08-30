package action_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestTestable_CaptureReq_And_ExpectErr(t *testing.T) {
	t.Parallel()

	builder := action.New("order.process", func(ctx context.Context, req int) (int, error) {
		if req <= 0 {
			return 0, xerr.Validation("quantity must be > 0")
		}
		return req * 10, nil
	}).HookBefore(func(ctx context.Context, _ int, _ *action.Meta) (context.Context, error) {
		return ctx, nil // Simulate middleware execution
	})

	testable := action.TestFrom(builder)

	// Validate ExpectErr
	err := testable.ExpectErr(context.Background(), 0, string(xerr.KindValidation))
	if err != nil {
		t.Fatalf("ExpectErr failed: %v", err)
	}

	// Validate CaptureReq
	captured, res, err := testable.CaptureReq(context.Background(), 5)
	if err != nil || captured != 5 || res != 50 {
		t.Fatalf("CaptureReq failed: cap=%v, res=%v, err=%v", captured, res, err)
	}
}

func TestTestable_DoRaw(t *testing.T) {
	t.Parallel()

	calls := 0
	builder := action.New("raw.test", func(ctx context.Context, req int) (int, error) {
		calls++
		return req * 2, nil
	}).HookBefore(func(ctx context.Context, _ int, _ *action.Meta) (context.Context, error) {
		t.Fatal("hook should not run in DoRaw")
		return ctx, nil
	})

	testable := action.TestFrom(builder)
	res, err := testable.DoRaw(context.Background(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != 20 {
		t.Fatalf("expected 20, got %d", res)
	}
	if calls != 1 {
		t.Fatalf("expected 1 handler call, got %d", calls)
	}
}

func TestTestable_ExpectErrPlainError(t *testing.T) {
	t.Parallel()

	builder := action.New("plain.err", func(ctx context.Context, req int) (int, error) {
		return 0, fmt.Errorf("plain failure")
	})
	testable := action.TestFrom(builder)

	// ExpectErr should return an error because the error is NOT an xerr.AppError
	if err := testable.ExpectErr(context.Background(), 1, string(xerr.KindValidation)); err == nil {
		t.Fatal("expected ExpectErr to return error for non-AppError")
	}
}
