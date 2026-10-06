// CoerceStringValue is a generic reflection primitive. It lives here
// because action is its first consumer; extract if a third independent
// package needs it without importing action.

package action

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

// CoerceStringValue converts a raw string into the value of target.
// Pointers are dereferenced and reallocated; time.Duration uses Go
// syntax; encoding.TextUnmarshaler types decode through the interface;
// []byte receives raw bytes; slices split on commas with empty
// elements dropped. Named types are preserved. Unsupported kinds error.
func CoerceStringValue(raw string, target reflect.Type) (any, error) {
	if target == nil {
		return nil, errors.New("coerce: nil target type")
	}
	return coerceValue(raw, target)
}

func coerceValue(raw string, target reflect.Type) (any, error) {
	if target.Kind() == reflect.Pointer {
		elem, err := coerceValue(raw, target.Elem())
		if err != nil {
			return nil, err
		}
		ptr := reflect.New(target.Elem())
		ptr.Elem().Set(reflect.ValueOf(elem))
		return ptr.Interface(), nil
	}

	if target == reflect.TypeFor[time.Duration]() {
		d, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", target, err)
		}
		return d, nil
	}

	if reflect.PointerTo(target).Implements(textUnmarshalerType) {
		return coerceTextUnmarshaler(raw, target)
	}

	if target.Kind() == reflect.Slice && target.Elem().Kind() == reflect.Uint8 {
		bytes := []byte(raw)
		out := reflect.MakeSlice(target, len(bytes), len(bytes))
		for i, b := range bytes {
			out.Index(i).SetUint(uint64(b))
		}
		return out.Interface(), nil
	}

	return coerceKind(raw, target)
}

func coerceTextUnmarshaler(raw string, target reflect.Type) (any, error) {
	ptr := reflect.New(target)
	unmarshaler, ok := reflect.TypeAssert[encoding.TextUnmarshaler](ptr)
	if !ok {
		return nil, fmt.Errorf("internal: %s does not implement encoding.TextUnmarshaler after assertion", target)
	}
	if err := unmarshaler.UnmarshalText([]byte(raw)); err != nil {
		return nil, err
	}
	return ptr.Elem().Interface(), nil
}

//nolint:exhaustive // default rejects unsupported kinds with a diagnostic
func coerceKind(raw string, target reflect.Type) (any, error) {
	trimmed := strings.TrimSpace(raw)
	switch target.Kind() {
	case reflect.String:
		return reflect.ValueOf(raw).Convert(target).Interface(), nil
	case reflect.Bool:
		v, err := strconv.ParseBool(trimmed)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", target, err)
		}
		return v, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(trimmed, 10, target.Bits())
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", target, err)
		}
		return reflect.ValueOf(v).Convert(target).Interface(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := strconv.ParseUint(trimmed, 10, target.Bits())
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", target, err)
		}
		return reflect.ValueOf(v).Convert(target).Interface(), nil
	case reflect.Float32, reflect.Float64:
		v, err := strconv.ParseFloat(trimmed, target.Bits())
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", target, err)
		}
		return reflect.ValueOf(v).Convert(target).Interface(), nil
	case reflect.Slice:
		return coerceSlice(raw, target)
	default:
		return nil, fmt.Errorf("unsupported kind %s", target.Kind())
	}
}

func coerceSlice(raw string, target reflect.Type) (any, error) {
	if raw == "" {
		return reflect.MakeSlice(target, 0, 0).Interface(), nil
	}
	parts := strings.Split(raw, ",")
	out := reflect.MakeSlice(target, 0, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := coerceValue(part, target.Elem())
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		out = reflect.Append(out, reflect.ValueOf(value))
	}
	return out.Interface(), nil
}
