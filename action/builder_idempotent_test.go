package action_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestBuilder_Idempotent(t *testing.T) {
	t.Parallel()
	act := action.New("idem.test", func(ctx context.Context, req string) (string, error) {
		return req, nil
	}).Idempotent().Build()

	meta := act.GetMeta()
	if !meta.Idempotency.Enabled {
		t.Fatal("expected idempotency to be enabled")
	}
	if meta.Idempotency.Header() != "Idempotency-Key" {
		t.Fatal("expected default header")
	}
}

func TestBuilder_IdempotentWithConfig(t *testing.T) {
	t.Parallel()
	act := action.New("idem.custom", func(ctx context.Context, req string) (string, error) {
		return req, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		TTL:       10 * time.Second,
		KeyHeader: "X-Key",
	}).Build()

	meta := act.GetMeta()
	if !meta.Idempotency.Enabled {
		t.Fatal("expected idempotency enabled")
	}
	if meta.Idempotency.Header() != "X-Key" {
		t.Fatalf("expected header X-Key, got %s", meta.Idempotency.Header())
	}
}
