package xctx

import (
	"context"
	"fmt"
	"sync/atomic"
)

var keySeq atomic.Uint64

// keyID is a pointer so boxing into any does not allocate.
// Boxing a struct allocates (runtime.convT2I); boxing a pointer does not.
type keyID struct {
	name string
	id   uint64
}

// Key is a typed context key. Two values are equal only when they came
// from the same NewKey. The zero value works in tests but allocates on
// With/From — use NewKey on hot paths.
type Key[T any] struct {
	id *keyID
}

// NewKey creates a typed context key. Every call returns a distinct key,
// even for the same name and the same type parameter.
func NewKey[T any](name string) Key[T] {
	return Key[T]{id: &keyID{name: name, id: keySeq.Add(1)}}
}

// With returns ctx with the value bound to this key.
func (k Key[T]) With(ctx context.Context, val T) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if k.id == nil {
		return context.WithValue(ctx, k, val)
	}
	return context.WithValue(ctx, k.id, val)
}

// From returns the bound value and true, or the zero value and false.
// Safe on a nil context and on a zero-value Key.
func (k Key[T]) From(ctx context.Context) (T, bool) {
	if ctx == nil {
		var zero T
		return zero, false
	}
	if k.id == nil {
		v, ok := ctx.Value(k).(T)
		return v, ok
	}
	v, ok := ctx.Value(k.id).(T)
	return v, ok
}

// MustFrom returns the bound value or panics. Use only at boot / startup
// where a missing value is a programmer error, never on a request path.
func (k Key[T]) MustFrom(ctx context.Context) T {
	v, ok := k.From(ctx)
	if !ok {
		panic(fmt.Sprintf("xctx: key %q not found in context", k.keyName()))
	}
	return v
}

func (k Key[T]) keyName() string {
	if k.id == nil {
		return ""
	}
	return k.id.name
}
