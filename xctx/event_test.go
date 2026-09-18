package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

type mockPublisher struct {
	lastSubject string
}

func (m *mockPublisher) PublishEvent(_ context.Context, subject string, _ any) error {
	m.lastSubject = subject
	return nil
}

func TestEventPublisher(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	if _, ok := xctx.EventPublisherFrom(ctx); ok {
		t.Fatal("expected EventPublisher to be missing initially")
	}

	pub := &mockPublisher{}
	ctx = xctx.WithEventPublisher(ctx, pub)

	retrieved, ok := xctx.EventPublisherFrom(ctx)
	if !ok || retrieved == nil {
		t.Fatal("expected EventPublisher in context")
	}

	_ = retrieved.PublishEvent(ctx, "order.created", nil)
	if pub.lastSubject != "order.created" {
		t.Fatalf("expected subject 'order.created', got %q", pub.lastSubject)
	}
}
