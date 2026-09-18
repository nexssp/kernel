package xctx

import (
	"context"
	"fmt"
)

// Key is a typed context key. Unlike a bare string, two Key values of
// different types never collide, and From returns a typed value without
// a runtime assertion at the callsite.
type Key[T any] struct {
	name string
}

// NewKey creates a typed context key. name is used only in error messages.
func NewKey[T any](name string) Key[T] {
	return Key[T]{name: name}
}

// With returns ctx with the value bound to this key.
func (k Key[T]) With(ctx context.Context, val T) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, k, val)
}

// From returns the bound value and true, or the zero value and false.
// Safe on a nil context.
func (k Key[T]) From(ctx context.Context) (T, bool) {
	if ctx == nil {
		var zero T
		return zero, false
	}
	v, ok := ctx.Value(k).(T)
	return v, ok
}

// MustFrom returns the bound value or panics. Use only at boot / startup
// where a missing value is a programmer error, never on a request path.
func (k Key[T]) MustFrom(ctx context.Context) T {
	v, ok := k.From(ctx)
	if !ok {
		panic(fmt.Sprintf("xctx: key %q not found in context", k.name))
	}
	return v
}
