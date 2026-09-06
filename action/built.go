package action

import (
	"context"
	"fmt"
)

func (a *BuiltAction[Req, Res]) DoAny(ctx context.Context, req any) (any, error) {
	// ⚡ 1 CPU cycle: direct type match (0 allocs)
	if typed, ok := req.(Req); ok {
		return a.Do(ctx, typed)
	}

	// Fast path: pointer dereference (*Req -> Req)
	if req != nil {
		if ptr, ok := req.(*Req); ok && ptr != nil {
			return a.Do(ctx, *ptr)
		}
	}

	// Nil handling for parameterless actions (struct{})
	if req == nil {
		var zero Req
		return a.Do(ctx, zero)
	}

	// Fallback coercion for cross-struct bridging
	typed, err := Coerce[Req](req)
	if err != nil {
		var zero Res
		return zero, fmt.Errorf("action [%s]: cannot coerce input %T into %T: %w", a.meta.Name, req, typed, err)
	}
	return a.Do(ctx, typed)
}

// ToBuilder converts a compiled action back to a Builder for further composition.
func (a *BuiltAction[Req, Res]) ToBuilder() *Builder[Req, Res] {
	b := New(a.meta.Name, a.exec).
		Description(a.meta.Description)
	for _, t := range a.meta.Tags {
		b.Tag(t)
	}
	return b
}
