package action_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

// ── 1. Mock Structures ────────────────────────────────────────────────────────

type mockTxRunner struct {
	called bool
}

func (m *mockTxRunner) RunInTx(ctx context.Context, fn func(txCtx context.Context) error) error {
	m.called = true
	return fn(ctx)
}

type mockAuditLogger struct {
	logged atomic.Bool
}

func (m *mockAuditLogger) Log(_ context.Context, _, _, _ string) {
	m.logged.Store(true)
}

type mockPIITracker struct {
	tracked atomic.Bool
}

func (m *mockPIITracker) TrackPIIAccess(_ context.Context, _, _ string) {
	m.tracked.Store(true)
}

type mockPublisher struct {
	lastSubject string
}

func (m *mockPublisher) PublishEvent(_ context.Context, subject string, _ any) error {
	m.lastSubject = subject
	return nil
}

// ── 2. Tests ──────────────────────────────────────────────────────────────────

func TestBuilder_Transactional(t *testing.T) {
	t.Parallel()
	runner := &mockTxRunner{}

	act := action.New("tx.test", func(_ context.Context, _ string) (string, error) {
		return "ok", nil
	}).Transactional(runner).Build()

	res, err := act.Do(context.Background(), "data")
	if err != nil || res != "ok" || !runner.called {
		t.Fatalf("expected tx to run, res=%q, err=%v, called=%v", res, err, runner.called)
	}
}

func TestBuilder_RequireCreationLimit(t *testing.T) {
	t.Parallel()

	checkFn := func(_ context.Context, _ string) (int64, int64, error) {
		return 10, 5, nil // Current 10 >= Limit 5 -> Exceeded
	}

	act := action.New("quota.test", func(_ context.Context, _ string) (string, error) {
		return "created", nil
	}).RequireCreationLimit("projects", checkFn).Build()

	ctx := xctx.WithTenantID(context.Background(), "tenant_1")
	_, err := act.Do(ctx, "req")
	if err == nil {
		t.Fatal("expected quota exceeded error, got nil")
	}
}

func TestBuilder_AuditedAndEvent(t *testing.T) {
	t.Parallel()
	audit := &mockAuditLogger{}
	pii := &mockPIITracker{}
	pub := &mockPublisher{}

	act := action.New("audit.event.test", func(_ context.Context, req string) (string, error) {
		return "res:" + req, nil
	}).
		Audited(audit, "BILLING", nil).
		TrackPIIAccess(pii, "user_invoice_generation").
		Emits("order.created", nil).
		Build()

	ctx := xctx.WithEventPublisher(context.Background(), pub)

	res, err := act.Do(ctx, "item")
	if err != nil || res != "res:item" {
		t.Fatalf("unexpected result: %v", err)
	}
	if !audit.logged.Load() {
		t.Fatal("expected audit log to be recorded")
	}
	if !pii.tracked.Load() {
		t.Fatal("expected PII access to be tracked")
	}
	if pub.lastSubject != "order.created" {
		t.Fatalf("expected event 'order.created', got %q", pub.lastSubject)
	}
}
