package action_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestBuilder_Idempotent(t *testing.T) {
	t.Parallel()
	act := action.New("idem.test", func(_ context.Context, req string) (string, error) {
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
	act := action.New("idem.custom", func(_ context.Context, req string) (string, error) {
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
	if meta.Idempotency.Namespace != "idem.custom" {
		t.Fatalf("expected final action namespace idem.custom, got %q", meta.Idempotency.Namespace)
	}
	//nolint:musttag // This test verifies encoding/json's current metadata shape.
	encoded, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal action metadata: %v", err)
	}
	if strings.Contains(string(encoded), "Store") || strings.Contains(string(encoded), "KeyFunc") {
		t.Fatalf("runtime idempotency configuration leaked into metadata: %s", encoded)
	}
}

func TestBuilder_IdempotencyNamespaceUsesFinalName(t *testing.T) {
	store := action.NewMemoryIdempotencyStore(time.Hour)
	builder := action.New("before.rename", func(_ context.Context, req string) (string, error) {
		return req, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{Store: store})
	builder.Name("after.rename")
	act := builder.Build()

	if got := act.GetMeta().Idempotency.Namespace; got != "after.rename" {
		t.Fatalf("idempotency namespace = %q, want after.rename", got)
	}
}
