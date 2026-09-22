package action

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
)

const paramTypeEnum = "enum"

// ParamSpec describes one external parameter of a NamedOperator.
type ParamSpec struct {
	Name    string
	Type    string
	Default any
	Usage   string
	Enum    []string // meaningful only when Type == "enum"
}

func (p ParamSpec) clone() ParamSpec {
	out := p
	if p.Enum != nil {
		out.Enum = append([]string(nil), p.Enum...)
	}
	return out
}

// StreamOperator is the runtime contract of a stream operator.
type StreamOperator interface {
	Name() string
	Apply(up AnyStream) (AnyStream, error)
}

// NamedOperator binds metadata to a StreamOperator builder.
type NamedOperator struct {
	Name        string
	Description string
	InType      reflect.Type
	OutType     reflect.Type
	Params      []ParamSpec
	Build       func(params map[string]any) (StreamOperator, error)
}

func (n NamedOperator) Clone() NamedOperator {
	out := n
	if n.Params != nil {
		out.Params = make([]ParamSpec, len(n.Params))
		for i, p := range n.Params {
			out.Params[i] = p.clone()
		}
	}
	return out
}

// allowedParamTypes is the closed set of DSL parameter types.
var allowedParamTypes = map[string]bool{
	"bool": true, "string": true, "int": true, "int64": true,
	"float64": true, "duration": true, paramTypeEnum: true,
}

// ValidateOperatorDeclaration checks the NamedOperator's own shape.
func ValidateOperatorDeclaration(op NamedOperator) error {
	if op.Name == "" {
		return errors.New("operator with empty name")
	}
	if op.Build == nil {
		return fmt.Errorf("operator %q: nil Build", op.Name)
	}
	if op.InType == nil || op.OutType == nil {
		return fmt.Errorf("operator %q: nil InType or OutType", op.Name)
	}
	seen := make(map[string]struct{}, len(op.Params))
	for i, p := range op.Params {
		if p.Name == "" {
			return fmt.Errorf("operator %q param #%d has empty name", op.Name, i)
		}
		if _, dup := seen[p.Name]; dup {
			return fmt.Errorf("operator %q declares param %q twice", op.Name, p.Name)
		}
		seen[p.Name] = struct{}{}
		if !allowedParamTypes[p.Type] {
			return fmt.Errorf("operator %q param %q has unknown type %q", op.Name, p.Name, p.Type)
		}
		if p.Type == paramTypeEnum && len(p.Enum) == 0 {
			return fmt.Errorf("operator %q param %q is enum but has no values", op.Name, p.Name)
		}
	}
	return nil
}

// ValidateOperatorParams checks DSL parameters against ParamSpec.
func ValidateOperatorParams(op NamedOperator, params map[string]any) (map[string]any, error) {
	declared := make(map[string]ParamSpec, len(op.Params))
	for _, p := range op.Params {
		declared[p.Name] = p
	}
	for k := range params {
		if _, ok := declared[k]; !ok {
			return nil, fmt.Errorf("operator %q does not accept parameter %q", op.Name, k)
		}
	}
	out := make(map[string]any, len(declared))
	for name, spec := range declared {
		val, has := params[name]
		if !has {
			if spec.Default != nil {
				out[name] = spec.Default
			}
			continue
		}
		if err := validateParamValue(op.Name, spec, val); err != nil {
			return nil, err
		}
		out[name] = val
	}
	return out, nil
}

func validateParamValue(opName string, spec ParamSpec, val any) error {
	switch spec.Type {
	case "bool":
		if _, ok := val.(bool); !ok {
			return paramTypeErr(opName, spec.Name, "bool", val)
		}
	case "string":
		if _, ok := val.(string); !ok {
			return paramTypeErr(opName, spec.Name, "string", val)
		}
	case "int":
		switch val.(type) {
		case int, int64:
		default:
			return paramTypeErr(opName, spec.Name, "int", val)
		}
	case "int64":
		if _, ok := val.(int64); !ok {
			return paramTypeErr(opName, spec.Name, "int64", val)
		}
	case "float64":
		if _, ok := val.(float64); !ok {
			return paramTypeErr(opName, spec.Name, "float64", val)
		}
	case "duration":
		if _, ok := val.(time.Duration); !ok {
			return paramTypeErr(opName, spec.Name, "duration", val)
		}
	case paramTypeEnum:
		return validateEnumParam(opName, spec, val)
	default:
		return fmt.Errorf("operator %q param %q: internal error, unknown type %q", opName, spec.Name, spec.Type)
	}
	return nil
}

func validateEnumParam(opName string, spec ParamSpec, val any) error {
	s, ok := val.(string)
	if !ok {
		return fmt.Errorf("operator %q param %q: enum requires string, got %T", opName, spec.Name, val)
	}
	if !slices.Contains(spec.Enum, s) {
		return fmt.Errorf("operator %q param %q: %q not in %v", opName, spec.Name, s, spec.Enum)
	}
	return nil
}

func paramTypeErr(opName, paramName, want string, got any) error {
	return fmt.Errorf("operator %q param %q: expected %s, got %T", opName, paramName, want, got)
}
