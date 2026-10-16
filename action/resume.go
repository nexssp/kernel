package action

import (
	"context"
	"errors"

	"github.com/nexssp/kernel/xerr"
)

var (
	ErrForceResumeRequired    = errors.New("execution resume: force resume is required")
	ErrForceReasonRequired    = errors.New("execution resume: force reason is required")
	ErrResumeIdentityRequired = errors.New("execution resume: identity function is required")
	ErrInvalidSuccessor       = errors.New("execution resume: invalid successor identity")
)

// ResumeOptions controls validation of a resumed execution.
type ResumeOptions struct {
	Force       bool
	ForceReason string
}

// ResumeJournal stores the identity binding alongside durable step records.
type ResumeJournal interface {
	Journal
	GetExecutionIdentity(ctx context.Context, executionID string) (ExecutionIdentity, bool, error)
	RecordExecutionIdentity(ctx context.Context, identity ExecutionIdentity) error
}

// ResumeIdentityFunc derives the binding for one durable request.
type ResumeIdentityFunc[Req any] func(context.Context, Req) (ExecutionIdentity, error)

// ValidateResume accepts an identical binding or validates an explicit successor.
// The successful comparison path is allocation-free.
func ValidateResume(existing, requested ExecutionIdentity, opts ResumeOptions) error {
	if requested.BindingEqual(existing) {
		return nil
	}
	mismatch := requested.ValidateBinding(existing)
	if !opts.Force {
		return xerr.Conflict(ErrForceResumeRequired.Error(), errors.Join(ErrForceResumeRequired, mismatch))
	}
	if opts.ForceReason == "" {
		return xerr.Validation(ErrForceReasonRequired.Error(), ErrForceReasonRequired)
	}
	if err := requested.Validate(); err != nil {
		return err
	}
	if requested.ExecutionID == existing.ExecutionID {
		if requested.Generation <= existing.Generation || requested.ParentExecutionID != existing.ExecutionID {
			return xerr.Validation(ErrInvalidSuccessor.Error(), ErrInvalidSuccessor)
		}
	} else if requested.ParentExecutionID != existing.ExecutionID {
		return xerr.Validation(ErrInvalidSuccessor.Error(), ErrInvalidSuccessor)
	}
	return nil
}

// DurableWithResume persists results only after validating their execution binding.
func DurableWithResume[Req, Res any](journal ResumeJournal, stepName string, identityFn ResumeIdentityFunc[Req], opts ResumeOptions) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		run := newDurableRunner(journal, stepName, next)

		return func(ctx context.Context, req Req) (Res, error) {
			var zero Res
			if journal == nil || stepName == "" {
				return next(ctx, req)
			}
			if identityFn == nil {
				return zero, xerr.Validation(ErrResumeIdentityRequired.Error(), ErrResumeIdentityRequired)
			}

			identity, identityErr := identityFn(ctx, req)
			if identityErr != nil {
				return zero, identityErr
			}
			if err := identity.Validate(); err != nil {
				return zero, err
			}

			existing, found, err := journal.GetExecutionIdentity(ctx, identity.ExecutionID)
			if err != nil {
				return zero, xerr.Unavailable("resume identity read failed", err)
			}
			if found {
				if err := ValidateResume(existing, identity, opts); err != nil {
					return zero, err
				}
				if !identity.BindingEqual(existing) {
					if err := journal.RecordExecutionIdentity(ctx, identity); err != nil {
						return zero, xerr.Unavailable("resume successor identity write failed", err)
					}
				}
			} else if err := journal.RecordExecutionIdentity(ctx, identity); err != nil {
				return zero, xerr.Unavailable("resume identity write failed", err)
			}

			return run(ctx, req, identity.JournalNamespace())
		}
	}
}
