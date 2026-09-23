package action

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

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
	data, err := json.Marshal(source)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// ValidateOperatorDeclaration validates operator completeness at registration time.
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
	return nil
}
