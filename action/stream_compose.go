// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nexssp/kernel/stream"
)

type typedStreamOperator[In, Out any] struct {
	name string
	op   stream.StreamOp[In, Out]
}

// NewTypedStreamOperator wraps a typed StreamOp as an erased StreamOperator.
func NewTypedStreamOperator[In, Out any](name string, op stream.StreamOp[In, Out]) StreamOperator {
	return &typedStreamOperator[In, Out]{name: name, op: op}
}

func (t *typedStreamOperator[In, Out]) Name() string { return t.name }

func (t *typedStreamOperator[In, Out]) Apply(up AnyStream) (AnyStream, error) {
	if up == nil {
		return nil, fmt.Errorf("operator %q: nil upstream", t.name)
	}
	typedUp := anyStreamToTyped[In](up, t.name)
	typedOut := t.op(typedUp)
	return typedStreamToAny[Out](typedOut), nil
}

func anyStreamToTyped[In any](up AnyStream, opName string) func(yield func(In, error) bool) {
	return func(yield func(In, error) bool) {
		up(func(item any, err error) bool {
			if err != nil {
				var zero In
				return yield(zero, err)
			}
			typed, ok := item.(In)
			if !ok {
				var zero In
				return yield(zero, fmt.Errorf("operator %q: expected %T, got %T", opName, zero, item))
			}
			return yield(typed, nil)
		})
	}
}

func typedStreamToAny[Out any](seq func(yield func(Out, error) bool)) AnyStream {
	return func(yield func(any, error) bool) {
		seq(func(item Out, err error) bool {
			return yield(any(item), err)
		})
	}
}

// ComposeOperators chains multiple operators into one.
func ComposeOperators(ops ...StreamOperator) (StreamOperator, error) {
	if len(ops) == 0 {
		return nil, errors.New("ComposeOperators requires at least one operator")
	}
	if len(ops) == 1 {
		return ops[0], nil
	}
	return &composedOperator{
		ops:  append([]StreamOperator(nil), ops...),
		name: "compose(" + joinArrow(ops) + ")",
	}, nil
}

type composedOperator struct {
	ops  []StreamOperator
	name string
}

func (c *composedOperator) Name() string { return c.name }

func (c *composedOperator) Apply(up AnyStream) (AnyStream, error) {
	cur := up
	for _, op := range c.ops {
		next, err := op.Apply(cur)
		if err != nil {
			return nil, fmt.Errorf("apply %q: %w", op.Name(), err)
		}
		cur = next
	}
	return cur, nil
}

func joinArrow(ops []StreamOperator) string {
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name()
	}
	return strings.Join(names, " -> ")
}
