package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestTypedKey(t *testing.T) {
	t.Parallel()
	key := xctx.NewKey[int]("test.int")

	ctx := key.With(context.Background(), 42)

	val, ok := key.From(ctx)
	if !ok || val != 42 {
		t.Fatalf("expected 42, got %v", val)
	}

	if val := key.MustFrom(ctx); val != 42 {
		t.Fatalf("MustFrom expected 42, got %v", val)
	}
}

func TestTypedKey_MustFromPanic(t *testing.T) {
	t.Parallel()
	key := xctx.NewKey[string]("test.missing")

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected MustFrom to panic on missing key")
		}
	}()

	_ = key.MustFrom(context.Background())
}
