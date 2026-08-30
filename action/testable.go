package action

import (
	"context"
	"errors"
	"fmt"

	"github.com/nexssp/kernel/xerr"
)

// Testable wraps a BuiltAction and provides helpers for unit testing.
type Testable[Req, Res any] struct {
	act     *BuiltAction[Req, Res]
	base    Fn[Req, Res]
	builder *Builder[Req, Res] // <-- nowe pole
}

func TestFrom[Req, Res any](b *Builder[Req, Res]) *Testable[Req, Res] {
	return &Testable[Req, Res]{
		act:     b.Build(),
		base:    b.exec,
		builder: b,
	}
}

// FromAction returns a Testable wrapper for a BuiltAction.
func TestFromAction[Req, Res any](act *BuiltAction[Req, Res]) *Testable[Req, Res] {
	return &Testable[Req, Res]{act: act}
}

// Do executes the full action (with all middleware/hooks).
func (t *Testable[Req, Res]) Do(ctx context.Context, req Req) (Res, error) {
	return t.act.Do(ctx, req)
}

// DoRaw executes only the base handler — no validation, no cache, no middleware.
func (t *Testable[Req, Res]) DoRaw(ctx context.Context, req Req) (Res, error) {
	return t.base(ctx, req)
}

// CaptureReq executes the action and captures the request as seen by the base handler.
// The captured value is the one after all middleware have run, right before calling exec.
func (t *Testable[Req, Res]) CaptureReq(ctx context.Context, input Req) (captured Req, res Res, err error) {
	var capturedReq Req

	// Wrap the base function — this is called after all middleware
	wrappedExec := func(ctx context.Context, req Req) (Res, error) {
		capturedReq = req // capture just before calling original
		return t.base(ctx, req)
	}

	if t.builder == nil {
		return capturedReq, res, fmt.Errorf("CaptureReq requires TestFrom with a builder")
	}

	b := New[Req, Res](t.act.meta.Name, wrappedExec)
	b.middlewares = append([]DispatcherMiddleware[Req, Res](nil), t.builder.middlewares...)
	b.hooks = append([]Hook[Req, Res](nil), t.builder.hooks...)
	b.anyHooks = append([]AnyHook(nil), t.builder.anyHooks...)
	b.bindings = append([]Binding(nil), t.builder.bindings...)

	built := b.Build()

	res, err = built.Do(ctx, input)
	return capturedReq, res, err
}

// ExpectErr checks if action returns expected error kind.
func (t *Testable[Req, Res]) ExpectErr(ctx context.Context, req Req, expected string) error {
	_, err := t.act.Do(ctx, req)
	if err == nil {
		return fmt.Errorf("expected error %q, got nil", expected)
	}
	var appErr *xerr.AppError
	if errors.As(err, &appErr) {
		if string(appErr.Kind) != expected {
			return fmt.Errorf("expected error kind %q, got %q", expected, appErr.Kind)
		}
		return nil
	}
	// Fail the test if the error is not a categorized xerr.AppError
	return fmt.Errorf("expected xerr.AppError kind %q, got non-app error: %v", expected, err)
}
