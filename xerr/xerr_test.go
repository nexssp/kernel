package xerr_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestXerr_Taxonomy(t *testing.T) {
	t.Parallel()

	err1 := xerr.NotFound("missing user")
	if err1.Kind != xerr.KindNotFound {
		t.Errorf("expected KindNotFound, got %s", err1.Kind)
	}
	ktest.RequirePermanent(t, err1)

	err2 := xerr.Unavailable("db down")
	if err2.Kind != xerr.KindUnavailable {
		t.Errorf("expected KindUnavailable, got %s", err2.Kind)
	}
	ktest.RequireTransient(t, err2)
}

// TestXerr_ClassificationIsTotalAndDisjoint pins the three-way
// classification of every Kind: none is both transient and permanent,
// and the six intentionally-unclassified kinds stay unclassified. This
// test fails loudly if anyone ever collapses classTransient and
// classPermanent to a boolean.
func TestXerr_ClassificationIsTotalAndDisjoint(t *testing.T) {
	t.Parallel()

	unclassified := map[xerr.Kind]bool{
		xerr.KindConflict:         true,
		xerr.KindInternal:         true,
		xerr.KindMethodNotAllowed: true,
		xerr.KindCanceled:         true,
		xerr.KindDatabase:         true,
		xerr.KindShutdown:         true,
	}

	for _, k := range xerr.AllKinds() {
		err := &xerr.AppError{Kind: k, Message: "test"}
		transient := xerr.IsTransient(err)
		permanent := xerr.IsPermanent(err)

		if transient && permanent {
			t.Errorf("kind %q is both transient and permanent", k)
			continue
		}
		if unclassified[k] {
			if transient || permanent {
				t.Errorf("kind %q must be unclassified; transient=%v permanent=%v",
					k, transient, permanent)
			}
			continue
		}
		if !transient && !permanent {
			t.Errorf("kind %q must be either transient or permanent", k)
		}
	}
}

func TestAllKinds_ReturnsEveryKind(t *testing.T) {
	t.Parallel()

	seen := make(map[xerr.Kind]bool)
	for _, k := range xerr.AllKinds() {
		if seen[k] {
			t.Errorf("AllKinds contains duplicate %q", k)
		}
		seen[k] = true
	}
	if len(seen) != 16 {
		t.Fatalf("AllKinds returns %d unique kinds, want 16", len(seen))
	}
}

func TestMapTransportError(t *testing.T) {
	t.Parallel()

	if got := xerr.MapTransportError(nil); got != nil {
		t.Fatalf("nil transport error mapped to %v", got)
	}

	mappedCancel := xerr.MapTransportError(context.Canceled)
	ktest.RequireErrorKind(t, mappedCancel, xerr.KindCanceled)

	mappedTimeout := xerr.MapTransportError(context.DeadlineExceeded)
	ktest.RequireErrorKind(t, mappedTimeout, xerr.KindTimeout)
}

func TestValidationDetails(t *testing.T) {
	t.Parallel()

	appErr := xerr.Validation("request is invalid")
	appErr.ValidationDetails = xerr.ValidationDetails{{
		Field: "Name", Validation: "required", Value: "missing",
	}}

	ktest.RequireErrorKind(t, appErr, xerr.KindValidation)
	if len(appErr.ValidationDetails) != 1 {
		t.Fatalf("expected 1 detail, got %d", len(appErr.ValidationDetails))
	}
	if got := appErr.ValidationDetails[0].String(); got != "[Name: required missing]" {
		t.Errorf("unexpected detail string: %q", got)
	}
}

func TestFromPublic(t *testing.T) {
	t.Parallel()

	response := xerr.ErrorResponse{
		Error:   string(xerr.KindValidation),
		Message: "request is invalid",
		Details: xerr.ValidationDetails{{Field: "email", Validation: "email"}},
	}
	got, ok := xerr.FromPublic(response)
	if !ok {
		t.Fatal("expected recognized remote error kind")
	}
	if got.Kind != xerr.KindValidation || got.Message != "request is invalid" {
		t.Fatalf("unexpected reconstructed error: %+v", got)
	}
	if got.Cause != nil || got.Stack != nil {
		t.Fatalf("remote errors must not restore internal fields: %+v", got)
	}
	if len(got.ValidationDetails) != 1 {
		t.Fatalf("expected validation details to survive, got %+v", got.ValidationDetails)
	}

	if _, ok := xerr.FromPublic(xerr.ErrorResponse{Error: "unrecognized"}); ok {
		t.Fatal("expected unrecognized remote error kind to be rejected")
	}
}

func TestFrom(t *testing.T) {
	t.Parallel()

	baseErr := errors.New("raw standard error")
	appErr := xerr.From(baseErr)

	ktest.RequireErrorKind(t, appErr, xerr.KindInternal)
	if !errors.Is(appErr.Unwrap(), baseErr) {
		t.Error("From should preserve the original cause")
	}

	// From on an existing *AppError must return it unchanged.
	if xerr.From(appErr) != appErr {
		t.Error("From on AppError should return itself without wrapping")
	}
}

// TestFrom_DelegatesToClassify confirms the shared classifier is used by
// From as well as MapTransportError, so a future edit to one cannot
// silently diverge from the other.
func TestFrom_DelegatesToClassify(t *testing.T) {
	t.Parallel()

	ktest.RequireErrorKind(t, xerr.From(context.Canceled), xerr.KindCanceled)
	ktest.RequireErrorKind(t, xerr.From(context.DeadlineExceeded), xerr.KindTimeout)
}

func TestMapTransportError_DNS(t *testing.T) {
	t.Parallel()
	dnsErr := &net.DNSError{Err: "no such host", Name: "example.invalid", IsNotFound: true}
	got := xerr.MapTransportError(dnsErr)

	ktest.RequireErrorKind(t, got, xerr.KindUnavailable)

	ae, ok := errors.AsType[*xerr.AppError](got)
	if !ok || !strings.Contains(ae.Message, "DNS") {
		t.Fatalf("expected DNS-specific message, got %q", got.Error())
	}
}
