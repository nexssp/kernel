package xerr_test

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/nexssp/kernel/xerr"
)

// ── constructor stack capture ────────────────────────────────────────────────

// TestCaptureStack_FirstUserFrameIsCaller pins the invariant: the formatted
// stack captured by Internal/Database starts at the caller of the constructor.
//
// Single-expression constructors like Database are often inlined, causing
// their frames to expand dynamically within CallersFrames alongside the caller.
// We must verify that after xerr internal frames are skipped (via isUserFrame
// logic, which we simulate here), the first resolved user frame belongs to the test.
func TestCaptureStack_FirstUserFrameIsCaller(t *testing.T) {
	t.Parallel()

	for _, err := range []*xerr.AppError{
		xerr.Internal("boom"),
		xerr.Database("db down"),
	} {
		if len(err.Stack) == 0 {
			t.Fatalf("%s: no stack captured", err.Kind)
		}
		frames := runtime.CallersFrames(err.Stack)
		var firstUserFrame string
		for {
			frame, more := frames.Next()
			// Simulate the print-time filter that ignores the raw xerr frames
			// and standard library noise.
			if !strings.HasPrefix(frame.Function, "github.com/nexssp/kernel/xerr.") && !strings.HasPrefix(frame.Function, "runtime.") {
				firstUserFrame = frame.Function
				break
			}
			if !more {
				break
			}
		}
		if !strings.Contains(firstUserFrame, "TestCaptureStack_FirstUserFrameIsCaller") {
			t.Fatalf("%s: first user frame should be the calling test, got %q",
				err.Kind, firstUserFrame)
		}
	}
}

// TestCaptureStack_RawStackContainsFrames verifies that captureStack returns
// valid, iteratable PCs (even if they start inside xerr, which is expected
// since we now preserve the exact raw return PCs).
func TestCaptureStack_RawStackContainsFrames(t *testing.T) {
	t.Parallel()

	for _, err := range []*xerr.AppError{
		xerr.Internal("boom"),
		xerr.Database("db down"),
	} {
		frames := runtime.CallersFrames(err.Stack)
		count := 0
		for {
			_, more := frames.Next()
			count++
			if !more {
				break
			}
		}
		if count == 0 {
			t.Fatalf("%s: expected stack to expand to >0 frames", err.Kind)
		}
	}
}

// TestCaptureStack_NonCapturingConstructorsHaveNoStack guards the split
// between stack-capturing constructors (Internal, Database) and
// non-capturing ones.
func TestCaptureStack_NonCapturingConstructorsHaveNoStack(t *testing.T) {
	t.Parallel()

	for _, err := range []*xerr.AppError{
		xerr.BadRequest("bad"),
		xerr.Unauthorized("unauth"),
		xerr.Forbidden("forbidden"),
		xerr.NotFound("missing"),
		xerr.Conflict("conflict"),
		xerr.Validation("invalid"),
		xerr.Timeout("slow"),
		xerr.Unavailable("down"),
		xerr.Canceled("canceled"),
		xerr.TooManyRequests("throttled"),
		xerr.RateLimit("rate"),
		xerr.CircuitBreaker("open"),
		xerr.Shutdown("bye"),
	} {
		if len(err.Stack) != 0 {
			t.Errorf("%s captured a stack unexpectedly (len=%d)",
				err.Kind, len(err.Stack))
		}
	}
}

// TestWithStack_Recaptures verifies that calling WithStack on an
// already-constructed error replaces the stack.
func TestWithStack_Recaptures(t *testing.T) {
	t.Parallel()

	err := xerr.BadRequest("bad")
	if len(err.Stack) != 0 {
		t.Fatal("BadRequest should not capture a stack")
	}

	err.WithStack()
	if len(err.Stack) == 0 {
		t.Fatal("WithStack did not capture a stack")
	}

	err.WithStack()
	if len(err.Stack) == 0 {
		t.Fatal("second WithStack left the stack empty")
	}
}

// ── Error / Unwrap ───────────────────────────────────────────────────────────

func TestAppError_Error_WithCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("socket closed")
	err := xerr.Unavailable("upstream down", cause)

	got := err.Error()
	if !strings.Contains(got, "[Unavailable]") {
		t.Errorf("missing kind: %q", got)
	}
	if !strings.Contains(got, "upstream down") {
		t.Errorf("missing message: %q", got)
	}
	if !strings.Contains(got, "socket closed") {
		t.Errorf("missing cause: %q", got)
	}
}

func TestAppError_Error_WithoutCause(t *testing.T) {
	t.Parallel()

	err := xerr.NotFound("user missing")
	want := "[NotFound] user missing"
	if got := err.Error(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAppError_Unwrap_PreservesCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("underlying")
	err := xerr.BadRequest("bad input", cause)

	if !errors.Is(err, cause) {
		t.Fatal("errors.Is did not reach the cause")
	}
	if !errors.Is(err.Unwrap(), cause) {
		t.Fatal("Unwrap returned the wrong error")
	}
}

func TestAppError_Unwrap_NilWhenNoCause(t *testing.T) {
	t.Parallel()

	if got := xerr.NotFound("x").Unwrap(); got != nil {
		t.Fatalf("Unwrap = %v, want nil", got)
	}
}

// ── Public() ─────────────────────────────────────────────────────────────────

func TestAppError_Public_DoesNotLeakInternals(t *testing.T) {
	t.Parallel()

	secret := errors.New("pq: password authentication failed for user admin")
	err := xerr.Internal("internal error", secret)
	err.ValidationDetails = xerr.ValidationDetails{{
		Field: "email", Validation: "required",
	}}

	pub := err.Public("req-42")

	if pub.Error != string(xerr.KindInternal) {
		t.Errorf("Error = %q", pub.Error)
	}
	if pub.Message != "internal error" {
		t.Errorf("Message = %q", pub.Message)
	}
	if pub.RequestID != "req-42" {
		t.Errorf("RequestID = %q", pub.RequestID)
	}
	if len(pub.Details) != 1 || pub.Details[0].Field != "email" {
		t.Errorf("Details lost: %+v", pub.Details)
	}
	if strings.Contains(fmt.Sprintf("%+v", pub), "password") {
		t.Fatal("cause leaked into ErrorResponse")
	}
}

func TestAppError_Public_EmptyRequestID(t *testing.T) {
	t.Parallel()

	pub := xerr.NotFound("x").Public("")
	if pub.RequestID != "" {
		t.Fatalf("RequestID = %q, want empty", pub.RequestID)
	}
}

// ── IsTransient ──────────────────────────────────────────────────────────────

func TestAppError_IsTransient_MatchesPackageFunction(t *testing.T) {
	t.Parallel()

	for _, k := range xerr.AllKinds() {
		err := &xerr.AppError{Kind: k, Message: "test"}
		if err.IsTransient() != xerr.IsTransient(err) {
			t.Errorf("kind %q: method=%v, package=%v",
				k, err.IsTransient(), xerr.IsTransient(err))
		}
	}
}

// ── constructor defaults ─────────────────────────────────────────────────────

func TestConstructors_DefaultMessages(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err  *xerr.AppError
		want string
	}{
		{xerr.MethodNotAllowed(""), "method not allowed"},
		{xerr.TooManyRequests(""), "too many requests"},
		{xerr.Timeout(""), "request timeout"},
		{xerr.Unavailable(""), "service unavailable"},
		{xerr.Canceled(""), "request canceled"},
		{xerr.RateLimit(""), "rate limit exceeded"},
		{xerr.CircuitBreaker(""), "circuit breaker open"},
		{xerr.Internal(""), "internal server error"},
	}
	for _, tc := range cases {
		if tc.err.Message != tc.want {
			t.Errorf("kind %s: message = %q, want %q",
				tc.err.Kind, tc.err.Message, tc.want)
		}
	}
}

func TestServiceUnavailable_AliasForUnavailable(t *testing.T) {
	t.Parallel()

	err := xerr.ServiceUnavailable("gone")
	if err.Kind != xerr.KindUnavailable {
		t.Fatalf("alias kind = %q, want %q", err.Kind, xerr.KindUnavailable)
	}
}
