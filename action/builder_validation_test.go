package action_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

func TestValidatePreservesAppErrorDetails(t *testing.T) {
	expected := xerr.Validation("validation failed")
	expected.ValidationDetails = xerr.ValidationDetails{{
		Field:      "session_id",
		Validation: "required",
		Value:      "missing",
	}}

	act := action.New("test.validate.details", func(_ context.Context, in string) (string, error) {
		return in, nil
	}).Validate(func(context.Context, string) error {
		return expected
	}).Build()

	_, err := act.Do(context.Background(), "input")
	if !errors.Is(err, expected) {
		t.Fatalf("expected original AppError, got %v", err)
	}
	got := xerr.From(err)
	if len(got.ValidationDetails) != 1 || got.ValidationDetails[0].Field != "session_id" {
		t.Fatalf("details were lost: %+v", got.ValidationDetails)
	}
}

func TestValidateWrapsOrdinaryErrors(t *testing.T) {
	act := action.New("test.validate.wrap", func(_ context.Context, in string) (string, error) {
		return in, nil
	}).Validate(func(context.Context, string) error {
		return errors.New("missing field")
	}).Build()

	_, err := act.Do(context.Background(), "input")
	if xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("expected validation kind, got %s", xerr.KindFrom(err))
	}
	if !strings.Contains(err.Error(), "missing field") {
		t.Fatalf("wrapped cause missing: %v", err)
	}
}

func TestValidatePreservesWrappedAppError(t *testing.T) {
	forbidden := xerr.Forbidden("policy denied")

	act := action.New("test.validate.wrapped", func(_ context.Context, in string) (string, error) {
		return in, nil
	}).Validate(func(context.Context, string) error {
		return fmt.Errorf("policy check: %w", forbidden)
	}).Build()

	_, err := act.Do(context.Background(), "input")
	if xerr.KindFrom(err) != xerr.KindForbidden {
		t.Fatalf("expected forbidden kind, got %s", xerr.KindFrom(err))
	}
}
