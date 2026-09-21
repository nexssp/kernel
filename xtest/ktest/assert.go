package ktest

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nexssp/kernel/xerr"
)

// RequireNoError fails the test when err is non-nil.
func RequireNoError(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatalf("unexpected error: %v", err)
	}
}

// RequireEqual fails the test when got and want differ.
func RequireEqual[T any](tb testing.TB, got, want T) {
	tb.Helper()
	if !reflect.DeepEqual(got, want) {
		tb.Fatalf("value mismatch\nwant: %+v\ngot:  %+v", want, got)
	}
}

// RequireErrorIs fails the test when err does not wrap target.
func RequireErrorIs(tb testing.TB, err, target error) {
	tb.Helper()
	if !errors.Is(err, target) {
		tb.Fatalf("expected error %v, got: %v", target, err)
	}
}

// RequireErrorContains fails the test when err is nil or lacks substr.
func RequireErrorContains(tb testing.TB, err error, substr string) {
	tb.Helper()
	if err == nil {
		tb.Fatalf("expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		tb.Fatalf("expected error containing %q, got: %v", substr, err)
	}
}

// RequireErrorKind fails the test unless err is an *xerr.AppError with
// the given kind. It prints the full error on failure so the caller does
// not need a second assertion to see what actually happened.
func RequireErrorKind(tb testing.TB, err error, want xerr.Kind) {
	tb.Helper()
	if err == nil {
		tb.Fatalf("expected kind %q, got nil error", want)
	}
	if got := xerr.KindFrom(err); got != want {
		tb.Fatalf("expected kind %q, got %q: %v", want, got, err)
	}
}

// RequireTransient fails the test unless err is transient by xerr rules
// (safe to retry).
func RequireTransient(tb testing.TB, err error) {
	tb.Helper()
	if !xerr.IsTransient(err) {
		tb.Fatalf("expected transient error, got %v", err)
	}
}

// RequirePermanent fails the test unless err is permanent by xerr rules
// (must not be retried).
func RequirePermanent(tb testing.TB, err error) {
	tb.Helper()
	if !xerr.IsPermanent(err) {
		tb.Fatalf("expected permanent error, got %v", err)
	}
}

// RequireStringContains fails the test when value lacks substr.
func RequireStringContains(tb testing.TB, value, substr string) {
	tb.Helper()
	if !strings.Contains(value, substr) {
		tb.Fatalf("expected %q to contain %q", value, substr)
	}
}

// RequireCondition fails the test when condition is false.
func RequireCondition(tb testing.TB, condition bool, format string, args ...any) {
	tb.Helper()
	if !condition {
		tb.Fatalf(format, args...)
	}
}
