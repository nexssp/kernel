package action

import (
	"context"
	"encoding/json"
	"fmt"
)

// Dynamic lifts any AnyAction into a *Builder[any, any].
// It allows composer.Pipe, composer.Parallel, and composer.Branch to orchestrate
// dynamic actions directly via InvokeAny.
func Dynamic(act AnyAction) *Builder[any, any] {
	if act == nil {
		panic("action.Dynamic: action cannot be nil")
	}

	desc := act.Describe()
	name := desc.Name
	if name == "" {
		name = "dynamic"
	}

	b := New(name, func(ctx context.Context, req any) (any, error) {
		return InvokeAny(ctx, act, req)
	}).
		Description(desc.Description)

	for _, t := range desc.Tags {
		b.Tag(t)
	}
	return b
}

// Coerce converts input into T using fast-path type assertions before falling back to JSON.
func Coerce[T any](input any) (T, error) {
	var zero T
	if input == nil {
		return zero, nil
	}

	// 1. Direct type match (Fastest: 1 CPU cycle, 0 allocs)
	if val, ok := input.(T); ok {
		return val, nil
	}

	// 2. String conversion fast-paths (common in AI prompts/pipes)
	var target T
	switch any(target).(type) {
	case string:
		switch v := input.(type) {
		case fmt.Stringer:
			return any(v.String()).(T), nil
		case []byte:
			return any(string(v)).(T), nil
		case error:
			return any(v.Error()).(T), nil
		}
	case []byte:
		switch v := input.(type) {
		case string:
			return any([]byte(v)).(T), nil
		case fmt.Stringer:
			return any([]byte(v.String())).(T), nil
		}
	}

	// 3. Map extraction: If target is string and input is a map containing common text keys
	if m, ok := input.(map[string]any); ok {
		if _, targetIsString := any(target).(string); targetIsString {
			for _, key := range []string{"content", "text", "output", "result", "message"} {
				if val, found := m[key]; found {
					if s, isStr := val.(string); isStr {
						return any(s).(T), nil
					}
				}
			}
		}
	}

	// 4. Fallback structural conversion only when bridging completely different structs
	data, err := json.Marshal(input)
	if err != nil {
		return zero, fmt.Errorf("coerce: marshal %T: %w", input, err)
	}
	var res T
	if err := json.Unmarshal(data, &res); err != nil {
		return zero, fmt.Errorf("coerce: unmarshal into %T: %w", zero, err)
	}
	return res, nil
}
