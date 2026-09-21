package xctx_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
)

// TestKey_SameKeyValueRoundTrips is the existing sanity check: a key
// stored and retrieved through the same variable works.
func TestKey_SameKeyValueRoundTrips(t *testing.T) {
	t.Parallel()
	k := xctx.NewKey[int]("k")
	ctx := k.With(context.Background(), 7)
	if v, ok := k.From(ctx); !ok || v != 7 {
		t.Fatalf("got %v, ok=%v", v, ok)
	}
}

// TestKey_IndependentNewKeyValuesDoNotCollide is the regression test
// for the pre-PR-4 bug: two independent NewKey calls with the same name
// and same type parameter must not share context values.
func TestKey_IndependentNewKeyValuesDoNotCollide(t *testing.T) {
	t.Parallel()

	a := xctx.NewKey[int]("user.id")
	b := xctx.NewKey[int]("user.id")

	ctx := a.With(context.Background(), 42)

	if v, ok := b.From(ctx); ok {
		t.Fatalf("distinct keys collided: b.From(ctx) = %d, ok=true", v)
	}
	// And the original still works.
	if v, ok := a.From(ctx); !ok || v != 42 {
		t.Fatalf("a.From(ctx) = %d, ok=%v", v, ok)
	}
}

// TestKey_DifferentTypesSameNameDoNotCollide pins the type-safety
// property that already worked pre-PR-4: identical name, different
// type parameter, no collision.
func TestKey_DifferentTypesSameNameDoNotCollide(t *testing.T) {
	t.Parallel()

	asInt := xctx.NewKey[int]("shared")
	asString := xctx.NewKey[string]("shared")

	ctx := asInt.With(context.Background(), 42)

	if v, ok := asString.From(ctx); ok {
		t.Fatalf("cross-type collision: asString.From = %q, ok=true", v)
	}
}

// TestKey_ZeroValueIsSafe confirms that a zero Key (never constructed
// by NewKey) still behaves: it stores and retrieves by struct equality,
// and two zero Key values of the same type are equal — which is what a
// caller who does `var k xctx.Key[int]` should expect.
func TestKey_ZeroValueIsSafe(t *testing.T) {
	t.Parallel()
	var k xctx.Key[int]

	ctx := k.With(context.Background(), 1)
	if v, ok := k.From(ctx); !ok || v != 1 {
		t.Fatalf("zero key round-trip failed: v=%v, ok=%v", v, ok)
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
