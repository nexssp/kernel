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

	// 1. O(1) Fast path: Direct assignability
	if sourceVal.Type().AssignableTo(targetVal.Elem().Type()) {
		targetVal.Elem().Set(sourceVal)
		return nil
	}
	if sourceVal.Kind() == reflect.Pointer && !sourceVal.IsNil() && sourceVal.Elem().Type().AssignableTo(targetVal.Elem().Type()) {
		targetVal.Elem().Set(sourceVal.Elem())
		return nil
	}

	// 2. String conversion fast-paths (common in AI prompts/pipes)
	if targetVal.Elem().Kind() == reflect.String {
		switch v := source.(type) {
		case string:
			targetVal.Elem().SetString(v)
			return nil
		case fmt.Stringer:
			targetVal.Elem().SetString(v.String())
			return nil
		case []byte:
			targetVal.Elem().SetString(string(v))
			return nil
		case error:
			targetVal.Elem().SetString(v.Error())
			return nil
		}

		// Map text-key extraction fallback
		if m, ok := source.(map[string]any); ok {
			for _, key := range DefaultTextFields {
				if val, found := m[key]; found {
					if s, isStr := val.(string); isStr {
						targetVal.Elem().SetString(s)
						return nil
					}
				}
			}
		}
	}

	// 3. Byte slice fast-paths
	if targetVal.Elem().Type() == reflect.TypeOf([]byte{}) {
		switch v := source.(type) {
		case string:
			targetVal.Elem().SetBytes([]byte(v))
			return nil
		case fmt.Stringer:
			targetVal.Elem().SetBytes([]byte(v.String()))
			return nil
		}
	}

	// 4. Fallback structural conversion
	data, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("coerce: marshal %T: %w", source, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("coerce: unmarshal into %T: %w", target, err)
	}
	return nil
}
