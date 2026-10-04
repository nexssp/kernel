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

// RequireStringNotContains fails the test when value contains substr.
func RequireStringNotContains(tb testing.TB, value, substr string) {
	tb.Helper()
	if strings.Contains(value, substr) {
		tb.Fatalf("expected %q to not contain %q", value, substr)
	}
}

// RequireCondition fails the test when condition is false.
func RequireCondition(tb testing.TB, condition bool, format string, args ...any) {
	tb.Helper()
	if !condition {
		tb.Fatalf(format, args...)
	}
}

// RequireErrorNotContains fails the test when err is nil or contains substr.
func RequireErrorNotContains(tb testing.TB, err error, substr string) {
	tb.Helper()
	if err == nil {
		tb.Fatalf("expected error not containing %q, got nil", substr)
	}
	if strings.Contains(err.Error(), substr) {
		tb.Fatalf("expected error to not contain %q, got: %v", substr, err)
	}
}

// RequireNotEqual fails the test when got and want are DeepEqual.
func RequireNotEqual[T any](tb testing.TB, got, want T) {
	tb.Helper()
	if reflect.DeepEqual(got, want) {
		tb.Fatalf("value unexpectedly equals %+v", want)
	}
}

// RequireNil fails when v is not nil, treating a typed nil pointer,
// slice, map, channel, func, or unsafe pointer as nil.
func RequireNil(tb testing.TB, v any) {
	tb.Helper()
	if !isNil(v) {
		tb.Fatalf("expected nil, got %#v (%T)", v, v)
	}
}

// RequireNotNil fails when v is nil, including the typed-nil case.
func RequireNotNil(tb testing.TB, v any) {
	tb.Helper()
	if isNil(v) {
		tb.Fatal("expected non-nil value")
	}
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only nilable kinds are checked; default falls through to false
	case reflect.Chan, reflect.Func, reflect.Map,
		reflect.Pointer, reflect.Slice, reflect.Interface,
		reflect.UnsafePointer:
		return rv.IsNil()
	}
	return false
}

// RequireTrue fails unless v is true.
func RequireTrue(tb testing.TB, v bool) {
	tb.Helper()
	if !v {
		tb.Fatal("expected true, got false")
	}
}

// RequireFalse fails unless v is false.
func RequireFalse(tb testing.TB, v bool) {
	tb.Helper()
	if v {
		tb.Fatal("expected false, got true")
	}
}

// RequireLen fails unless len(v) == want for a slice, array, map,
// channel, or string. A nil interface is treated as a zero-length
// collection when want == 0.
func RequireLen(tb testing.TB, v any, want int) {
	tb.Helper()
	if v == nil {
		if want == 0 {
			return
		}
		tb.Fatalf("expected length %d, got nil", want)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only lengthed kinds are supported
	case reflect.Array, reflect.Chan, reflect.Map,
		reflect.Slice, reflect.String:
		if got := rv.Len(); got != want {
			tb.Fatalf("expected length %d, got %d (%T = %v)", want, got, v, v)
		}
	default:
		tb.Fatalf("RequireLen: %T is not a collection", v)
	}
}

// RequirePanics fails when fn does not panic and returns the recovered
// value on success.
func RequirePanics(tb testing.TB, fn func()) any {
	tb.Helper()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		fn()
	}()
	if recovered == nil {
		tb.Fatal("expected function to panic")
	}
	return recovered
}

// RequireNotPanics fails when fn panics, showing the recovered value.
func RequireNotPanics(tb testing.TB, fn func()) {
	tb.Helper()
	defer func() {
		if r := recover(); r != nil {
			tb.Fatalf("unexpected panic: %v", r)
		}
	}()
	fn()
}
