package action

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"

	"github.com/nexssp/kernel/stream"
)

// StreamOperator is the runtime contract of an execution operator.
type StreamOperator interface {
	Name() string
	Apply(up AnyStream) (AnyStream, error)
}

// NamedOperator binds metadata to an operator constructor.
type NamedOperator struct {
	Name        string
	Description string
	InType      reflect.Type
	OutType     reflect.Type
	ConfigType  reflect.Type
	Build       func(params any) (StreamOperator, error)
}

func (n NamedOperator) Clone() NamedOperator {
	return n
}

// NewOperator creates a strongly-typed stream operator from a config struct Cfg.
func NewOperator[In, Out, Cfg any](
	name string,
	factory func(cfg Cfg) stream.StreamOp[In, Out],
) NamedOperator {
	return NamedOperator{
		Name:       name,
		InType:     reflect.TypeFor[In](),
		OutType:    reflect.TypeFor[Out](),
		ConfigType: reflect.TypeFor[Cfg](),
		Build: func(raw any) (StreamOperator, error) {
			var cfg Cfg
			if raw != nil {
				if err := decodeConfig(&cfg, raw); err != nil {
					return nil, fmt.Errorf("operator %q config error: %w", name, err)
				}
			}
			return NewTypedStreamOperator(name, factory(cfg)), nil
		},
	}
}

// NewSimpleOperator creates an operator that requires no configuration.
func NewSimpleOperator[In, Out any](
	name string,
	op stream.StreamOp[In, Out],
) NamedOperator {
	return NewOperator(name, func(_ struct{}) stream.StreamOp[In, Out] {
		return op
	})
}

func decodeConfig(target, source any) error {
	normalized, err := normalizeConfigForTarget(source, target)
	if err != nil {
		return err
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// normalizeConfigForTarget converts string values from a raw config map
// into the JSON-compatible representation expected by the target
// struct's field types. This is the whole-struct counterpart to
// CoerceStringValue: config decoding calls it once per operator build,
// so a string "false" for a bool field reaches the JSON decoder as a
// real bool, and a string "false" for a string field stays a string.
//
// Fields not present in the source map are left untouched — the JSON
// decoder applies zero values or explicit defaults. Unknown keys in
// the source map are passed through so DisallowUnknownFields in the
// decoder can reject them with its own diagnostic.
func normalizeConfigForTarget(source, target any) (any, error) {
	m, ok := source.(map[string]any)
	if !ok || target == nil {
		return source, nil
	}
	typ := reflect.TypeOf(target)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return source, nil
	}

	out := make(map[string]any, len(m))
	maps.Copy(out, m)

	for field := range typ.Fields() {
		if !field.IsExported() {
			continue
		}
		jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if jsonName == "" {
			jsonName = field.Name
		}
		if jsonName == "-" {
			continue
		}
		raw, exists := out[jsonName]
		if !exists {
			continue
		}
		str, isStr := raw.(string)
		if !isStr {
			continue
		}
		coerced, err := CoerceStringValue(str, field.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", jsonName, err)
		}
		out[jsonName] = coerced
	}
	return out, nil
}

// ValidateOperatorDeclaration validates operator completeness at
// registration time. Every field that a caller could observe is
// checked, so a hand-constructed NamedOperator with a nil ConfigType
// is rejected here rather than at the first build attempt.
func ValidateOperatorDeclaration(op NamedOperator) error {
	if op.Name == "" {
		return errors.New("operator with empty name")
	}
	if op.Build == nil {
		return fmt.Errorf("operator %q: nil Build", op.Name)
	}
	if op.InType == nil {
		return fmt.Errorf("operator %q: nil InType", op.Name)
	}
	if op.OutType == nil {
		return fmt.Errorf("operator %q: nil OutType", op.Name)
	}
	if op.ConfigType == nil {
		return fmt.Errorf("operator %q: nil ConfigType", op.Name)
	}
	return nil
}
