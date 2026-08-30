package action_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

// TestAction_HooksOrder verifies Before hooks run FIFO and After hooks run LIFO (Cleanup pattern).
func TestAction_HooksOrder(t *testing.T) {
	t.Parallel()

	var execution strings.Builder

	act := action.New("test.order", func(ctx context.Context, req string) (string, error) {
		execution.WriteString("EXEC;")
		return "ok", nil
	}).
		Hook(action.Hook[string, string]{
			Before: func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
				execution.WriteString("B1;")
				return ctx, nil
			},
			After: func(ctx context.Context, r, res string, err error, m *action.Meta) {
				execution.WriteString("A1;")
			},
		}).
		Hook(action.Hook[string, string]{
			Before: func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
				execution.WriteString("B2;")
				return ctx, nil
			},
			After: func(ctx context.Context, r, res string, err error, m *action.Meta) {
				execution.WriteString("A2;")
			},
		}).
		Build()

	_, err := act.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "B1;B2;EXEC;A2;A1;"
	if execution.String() != expected {
		t.Fatalf("expected hook order %q, got %q", expected, execution.String())
	}
}

func TestAction_PanicRecovery(t *testing.T) {
	t.Parallel()

	act := action.New("test.panic", func(ctx context.Context, req int) (int, error) {
		panic("database connection lost")
	}).Build()

	_, err := act.Do(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	if !strings.Contains(err.Error(), "database connection lost") {
		t.Fatalf("expected panic message in error, got: %v", err)
	}
}

func TestExecuteDecoded(t *testing.T) {
	t.Parallel()

	type Req struct{ Name string }
	act := action.New("test.decode", func(ctx context.Context, req *Req) (string, error) {
		return req.Name, nil
	}).Build()

	res, err := act.ExecuteDecoded(context.Background(), func(v any) error {
		req, ok := v.(*Req)
		if !ok {
			return fmt.Errorf("expected *Req, got %T", v)
		}
		*req = Req{Name: "Decoded"}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := res.(string)
	if !ok || got != "Decoded" {
		t.Fatalf("expected 'Decoded', got %v", res)
	}
}

func TestAnyHook_OnPanic_Fires(t *testing.T) {
	t.Parallel()
	var recoveredVal any

	act := action.New("critical.db.write", func(ctx context.Context, req int) (int, error) {
		panic("database connection melted")
	}).AnyHook(action.AnyHook{
		OnPanic: func(ctx context.Context, req any, recovered any, meta *action.Meta) {
			recoveredVal = recovered
		},
	}).Build()

	_, err := act.Do(context.Background(), 1)
	if err == nil {
		t.Fatal("expected panic to be recovered into an error")
	}

	if recoveredVal != "database connection melted" {
		t.Fatalf("expected OnPanic hook to receive panic value, got %v", recoveredVal)
	}
}

var errRejected = errors.New("rejected")

func TestBeforeHook_Abort(t *testing.T) {
	t.Parallel()

	act := action.New("abort.test", func(ctx context.Context, req string) (string, error) {
		t.Fatal("handler should not have been called")
		return "never", nil
	}).
		HookBefore(func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
			return ctx, errRejected
		}).
		Build()

	_, err := act.Do(context.Background(), "req")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Idiomatic Go: errors.Is automatically unwraps the fmt.Errorf("%w") chain
	if !errors.Is(err, errRejected) {
		t.Fatalf("expected error chain to contain errRejected, got: %v", err)
	}

	// Optional: Verify the framework correctly attached the action context
	if !strings.Contains(err.Error(), "action abort.test before-hook failed") {
		t.Fatalf("expected framework to wrap error with action context, got: %v", err)
	}
}

func TestMixedHooks_Order(t *testing.T) {
	t.Parallel()

	var execution strings.Builder

	act := action.New("mixed.order", func(ctx context.Context, req string) (string, error) {
		execution.WriteString("EXEC;")
		return "ok", nil
	}).
		Hook(action.Hook[string, string]{
			Before: func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
				execution.WriteString("TB1;")
				return ctx, nil
			},
			After: func(ctx context.Context, r, res string, err error, m *action.Meta) {
				execution.WriteString("TA1;")
			},
		}).
		AnyHook(action.AnyHook{
			Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
				execution.WriteString("AB1;")
				return ctx, nil
			},
			After: func(ctx context.Context, r, res any, err error, m *action.Meta) {
				execution.WriteString("AA1;")
			},
		}).
		Hook(action.Hook[string, string]{
			Before: func(ctx context.Context, _ string, _ *action.Meta) (context.Context, error) {
				execution.WriteString("TB2;")
				return ctx, nil
			},
			After: func(ctx context.Context, r, res string, err error, m *action.Meta) {
				execution.WriteString("TA2;")
			},
		}).
		AnyHook(action.AnyHook{
			Before: func(ctx context.Context, r any, m *action.Meta) (context.Context, error) {
				execution.WriteString("AB2;")
				return ctx, nil
			},
			After: func(ctx context.Context, r, res any, err error, m *action.Meta) {
				execution.WriteString("AA2;")
			},
		}).
		Build()

	_, err := act.Do(context.Background(), "req")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "AB1;AB2;TB1;TB2;EXEC;TA2;TA1;AA2;AA1;"
	if execution.String() != expected {
		t.Fatalf("expected order %q, got %q", expected, execution.String())
	}
}
