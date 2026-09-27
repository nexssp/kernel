// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
)

// InvokeAny executes an action with an in-memory input payload.
func InvokeAny(ctx context.Context, act, req any) (any, error) {
	if act == nil {
		return nil, errors.New("action: cannot invoke nil action")
	}

	if invoker, ok := act.(AnyDoer); ok {
		return invoker.DoAny(ctx, req)
	}

	if exec, ok := act.(Executable); ok {
		return exec.ExecuteDecoded(ctx, func(target any) error {
			if req == nil {
				return nil
			}
			return Assign(target, req)
		})
	}

	return nil, fmt.Errorf("action: type %T does not implement Invoker or Executable", act)
}

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

	// Preserve bindings (transport metadata).
	//
	// Hooks are intentionally NOT copied here: InvokeAny dispatches through
	// the wrapped action's Do method, which fires the wrapped action's own
	// hooks. Re-registering them on this wrapper would run every hook twice
	// per call. Callers who want to *add* hooks around the wrapper should
	// use .AnyHook(...) on the returned builder.
	b.bindings = append(b.bindings, act.GetBindings()...)

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
func Assign(target, source any) error {
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
	if assignReflect(targetVal, sourceVal) {
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

func assignReflect(target, source reflect.Value) bool {
	return assignValue(target.Elem(), source)
}

func assignValue(dst, src reflect.Value) bool {
	src = indirect(src)
	if !src.IsValid() {
		return false
	}

	if dst.Kind() == reflect.Interface {
		dst.Set(src)
		return true
	}
	if dst.Kind() == reflect.Pointer {
		if dst.IsNil() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		return assignValue(dst.Elem(), src)
	}

	if src.Type().AssignableTo(dst.Type()) {
		dst.Set(src)
		return true
	}

	return assignByKind(dst, src)
}

// indirect unwraps pointers and interfaces, returning the zero Value for a nil chain.
func indirect(src reflect.Value) reflect.Value {
	for src.Kind() == reflect.Pointer || src.Kind() == reflect.Interface {
		if src.IsNil() {
			return reflect.Value{}
		}
		src = src.Elem()
	}
	return src
}

func assignByKind(dst, src reflect.Value) bool {
	//nolint:exhaustive // only kinds routable via reflection are handled; others fall through to false
	switch dst.Kind() {
	case reflect.Struct:
		return assignStruct(dst, src)
	case reflect.Slice:
		return assignSlice(dst, src)
	case reflect.Map:
		return assignMap(dst, src)
	case reflect.String:
		if s, ok := stringFromValue(src); ok {
			dst.SetString(s)
			return true
		}
	case reflect.Bool:
		if src.Kind() == reflect.Bool {
			dst.SetBool(src.Bool())
			return true
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if isNumericKind(src.Kind()) {
			dst.SetInt(toInt64(src))
			return true
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if isNumericKind(src.Kind()) {
			n := toInt64(src)
			if n < 0 {
				return false
			}
			dst.SetUint(uint64(n))
			return true
		}
	case reflect.Float32, reflect.Float64:
		if isNumericKind(src.Kind()) {
			dst.SetFloat(toFloat64(src))
			return true
		}
	default:
		return false
	}
	return false
}

type structIndex struct {
	byJSONKey map[string]int
	byName    map[string]int
}

var structIndexCache sync.Map

func lookupStructIndex(t reflect.Type) *structIndex {
	if v, ok := structIndexCache.Load(t); ok {
		if idx, ok := v.(*structIndex); ok {
			return idx
		}
	}
	idx := &structIndex{
		byJSONKey: make(map[string]int, t.NumField()),
		byName:    make(map[string]int, t.NumField()),
	}
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		idx.byName[f.Name] = i
		tag := f.Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		if comma := strings.IndexByte(tag, ','); comma >= 0 {
			tag = tag[:comma]
		}
		if tag != "" {
			idx.byJSONKey[tag] = i
		}
	}
	structIndexCache.Store(t, idx)
	return idx
}

func assignStruct(dst, src reflect.Value) bool {
	if src.Kind() != reflect.Map || src.Type().Key().Kind() != reflect.String {
		return false
	}
	idx := lookupStructIndex(dst.Type())
	iter := src.MapRange()
	for iter.Next() {
		key := iter.Key().String()
		fieldIdx, ok := idx.byJSONKey[key]
		if !ok {
			fieldIdx, ok = idx.byName[key]
			if !ok {
				continue
			}
		}
		field := dst.Field(fieldIdx)
		if !field.CanSet() {
			continue
		}
		if !assignValue(field, iter.Value()) {
			return false
		}
	}
	return true
}

func assignSlice(dst, src reflect.Value) bool {
	if src.Kind() != reflect.Slice && src.Kind() != reflect.Array {
		return false
	}
	n := src.Len()
	out := reflect.MakeSlice(dst.Type(), n, n)
	elemType := dst.Type().Elem()
	for i := range n {
		sv := src.Index(i)
		ev := out.Index(i)
		if assignValue(ev, sv) {
			continue
		}
		for sv.Kind() == reflect.Interface && !sv.IsNil() {
			sv = sv.Elem()
		}
		if !sv.Type().AssignableTo(elemType) {
			return false
		}
		ev.Set(sv)
	}
	dst.Set(out)
	return true
}

func assignMap(dst, src reflect.Value) bool {
	if src.Kind() != reflect.Map || src.Type().Key().Kind() != reflect.String {
		return false
	}
	if dst.Type().Key().Kind() != reflect.String {
		return false
	}
	out := reflect.MakeMap(dst.Type())
	elemType := dst.Type().Elem()
	iter := src.MapRange()
	for iter.Next() {
		elemPtr := reflect.New(elemType)
		if !assignValue(elemPtr.Elem(), iter.Value()) {
			sv := iter.Value()
			for sv.Kind() == reflect.Interface && !sv.IsNil() {
				sv = sv.Elem()
			}
			if !sv.Type().AssignableTo(elemType) {
				return false
			}
			elemPtr.Elem().Set(sv)
		}
		out.SetMapIndex(iter.Key(), elemPtr.Elem())
	}
	dst.Set(out)
	return true
}

func isNumericKind(k reflect.Kind) bool {
	//nolint:exhaustive // only numeric kinds are recognized; everything else is not numeric
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func toInt64(v reflect.Value) int64 {
	//nolint:exhaustive // only numeric kinds are converted; everything else returns 0
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint()) //nolint:gosec // best-effort numeric coercion; large uint64 truncates, same as encoding/json
	case reflect.Float32, reflect.Float64:
		return int64(v.Float())
	default:
		return 0
	}
}

func toFloat64(v reflect.Value) float64 {
	//nolint:exhaustive // only numeric kinds are converted; everything else returns 0
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		return v.Float()
	default:
		return 0
	}
}

func stringFromValue(v reflect.Value) (string, bool) {
	//nolint:exhaustive // only string and []byte are convertible; everything else returns false
	switch v.Kind() {
	case reflect.String:
		return v.String(), true
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return string(v.Bytes()), true
		}
	default:
		return "", false
	}
	return "", false
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
