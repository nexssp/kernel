package xerr_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/xerr"
)

func TestXerr_Taxonomy(t *testing.T) {
	t.Parallel()

	err1 := xerr.NotFound("missing user")
	if err1.Kind != xerr.KindNotFound {
		t.Errorf("expected KindNotFound, got %s", err1.Kind)
	}
	if xerr.IsTransient(err1) {
		t.Error("NotFound should not be transient")
	}

	err2 := xerr.Unavailable("db down")
	if err2.Kind != xerr.KindUnavailable {
		t.Errorf("expected KindUnavailable, got %s", err2.Kind)
	}
	if !xerr.IsTransient(err2) {
		t.Error("Unavailable should be transient")
	}
}

func TestMapTransportError(t *testing.T) {
	t.Parallel()

	if got := xerr.MapTransportError(nil); got != nil {
		t.Fatalf("nil transport error mapped to %v", got)
	}

	mappedCancel := xerr.MapTransportError(context.Canceled)
	if xerr.KindFrom(mappedCancel) != xerr.KindCanceled {
		t.Errorf("expected Canceled, got %s", xerr.KindFrom(mappedCancel))
	}

	mappedTimeout := xerr.MapTransportError(context.DeadlineExceeded)
	if xerr.KindFrom(mappedTimeout) != xerr.KindTimeout {
		t.Errorf("expected Timeout, got %s", xerr.KindFrom(mappedTimeout))
	}
}

func TestValidationDetails(t *testing.T) {
	t.Parallel()

	appErr := xerr.Validation("request is invalid")
	appErr.ValidationDetails = xerr.ValidationDetails{{
		Field: "Name", Validation: "required", Value: "missing",
	}}

	if appErr.Kind != xerr.KindValidation {
		t.Errorf("expected KindValidation, got %s", appErr.Kind)
	}
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

	if appErr.Kind != xerr.KindInternal {
		t.Errorf("raw errors should be mapped to Internal, got %s", appErr.Kind)
	}
	if !errors.Is(appErr.Unwrap(), baseErr) {
		t.Error("From should preserve the original cause")
	}

	// Converting an already-converted error should return itself
	appErr2 := xerr.From(appErr)
	if appErr2 != appErr {
		t.Error("From on AppError should return itself without wrapping")
	}
}
