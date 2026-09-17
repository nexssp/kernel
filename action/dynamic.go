package action

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
)

var DefaultTextFields = []string{"content", "text", "output", "result", "message"}

// Dynamic lifts any AnyAction into a *Builder[any, any].
// It preserves all metadata, bindings, and hooks from the wrapped action.
func Dynamic(act AnyAction) *Builder[any, any] {
	if act == nil {
		panic("action.Dynamic: action cannot be nil")
	}

	b := New(act.Describe().Name, func(ctx context.Context, req any) (any, error) {
		return InvokeAny(ctx, act, req)
	})

	if desc := act.Describe(); desc != nil {
		b.meta = *desc
		b.meta.Tags = slices.Clone(desc.Tags)
		b.meta.RequiredRoles = slices.Clone(desc.RequiredRoles)
		b.meta.RequiredPermissions = slices.Clone(desc.RequiredPermissions)
		b.meta.RequiredFeatures = slices.Clone(desc.RequiredFeatures)
	}

	// Preserve bindings and hooks
	b.bindings = append(b.bindings, act.GetBindings()...)
	b.anyHooks = append(b.anyHooks, act.GetAnyHooks()...)

	return b
}

// Coerce converts input into T.
func Coerce[T any](input any) (T, error) {
	var target T
	err := Assign(&target, input)
	return target, err
}

// Assign writes 'source' into the pointer 'target' using zero-allocation fast paths
// before falling back to JSON serialization. 'target' MUST be a non-nil pointer.
func Assign(target any, source any) error {
	if source == nil || target == nil {
		return nil
	}

	targetVal := reflect.ValueOf(target)
	if targetVal.Kind() != reflect.Pointer || targetVal.IsNil() {
		return fmt.Errorf("coerce: target must be a non-nil pointer, got %T", target)
	}

	sourceVal := reflect.ValueOf(source)

	if assignDirect(targetVal, sourceVal) {
		return nil
	}
	if assignString(targetVal, source) {
		return nil
	}
	if assignBytes(targetVal, source) {
		return nil
	}

	return assignJSON(target, source)
}

func assignDirect(target, source reflect.Value) bool {
	if source.Type().AssignableTo(target.Elem().Type()) {
		target.Elem().Set(source)
		return true
	}
	if source.Kind() == reflect.Pointer && !source.IsNil() &&
		source.Elem().Type().AssignableTo(target.Elem().Type()) {
		target.Elem().Set(source.Elem())
		return true
	}
	return false
}

func assignString(target reflect.Value, source any) bool {
	if target.Elem().Kind() != reflect.String {
		return false
	}
	switch v := source.(type) {
	case string:
		target.Elem().SetString(v)
		return true
	case fmt.Stringer:
		target.Elem().SetString(v.String())
		return true
	case []byte:
		target.Elem().SetString(string(v))
		return true
	case error:
		target.Elem().SetString(v.Error())
		return true
	}
	if m, ok := source.(map[string]any); ok {
		for _, key := range DefaultTextFields {
			if val, found := m[key]; found {
				if s, isStr := val.(string); isStr {
					target.Elem().SetString(s)
					return true
				}
			}
		}
	}
	return false
}

func assignBytes(target reflect.Value, source any) bool {
	if target.Elem().Type() != reflect.TypeFor[[]byte]() {
		return false
	}
	switch v := source.(type) {
	case string:
		target.Elem().SetBytes([]byte(v))
		return true
	case fmt.Stringer:
		target.Elem().SetBytes([]byte(v.String()))
		return true
	}
	return false
}

func assignJSON(target, source any) error {
	data, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("coerce: marshal %T: %w", source, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("coerce: unmarshal into %T: %w", target, err)
	}
	return nil
}
