package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

func TestApprovalToken_RoundTrip(t *testing.T) {
	ctx := xctx.WithApprovalToken(context.Background(), "tok-abc")
	if got := xctx.ApprovalTokenFrom(ctx); got != "tok-abc" {
		t.Fatalf("got %q, want tok-abc", got)
	}
}

func TestApprovalToken_MissingReturnsEmpty(t *testing.T) {
	if got := xctx.ApprovalTokenFrom(context.Background()); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestApprovalToken_Overwrite(t *testing.T) {
	ctx := xctx.WithApprovalToken(context.Background(), "first")
	ctx = xctx.WithApprovalToken(ctx, "second")
	if got := xctx.ApprovalTokenFrom(ctx); got != "second" {
		t.Fatalf("got %q, want second", got)
	}
}

func TestApprovalToken_NilContext(t *testing.T) {
	// Callers occasionally pass a zero-value context through. Extractor
	// must not panic.
	//nolint:staticcheck // explicit test of the nil-context path
	if got := xctx.ApprovalTokenFrom(nil); got != "" {
		t.Fatalf("nil ctx: got %q, want empty", got)
	}
}
