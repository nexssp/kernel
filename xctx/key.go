package xctx

import (
	"context"
	"fmt"
	"sync/atomic"
)

// keySeq is the process-wide counter that gives each Key a unique
// identity. Two Key values constructed by separate NewKey calls are
// distinct keys even when they share a name, so two packages that
// independently name their key "user_id" cannot accidentally share
// context values.
var keySeq atomic.Uint64

// Key is a typed context key. Two Key values are equal only when they
// came from the same NewKey call. The name is used for error messages
// only; identity comes from an internal counter.
type Key[T any] struct {
	name string
	id   uint64
}

// NewKey creates a typed context key. Every call returns a distinct
// key, even for the same name and the same type parameter.
func NewKey[T any](name string) Key[T] {
	return Key[T]{name: name, id: keySeq.Add(1)}
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
