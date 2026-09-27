package ktest

import (
	"reflect"
	"testing"

	"github.com/nexssp/kernel/action"
)

// AssertContracts checks structural invariants across every action and is
// intentionally soft: each action is validated in its own subtest using
// t.Errorf so that one bad action does not hide the rest.
func AssertContracts(t *testing.T, actions []action.AnyAction) {
	t.Helper()

	names := make(map[string]bool)

	for _, act := range actions {
		if act == nil {
			t.Error("nil action encountered in AssertContracts")
			continue
		}
		meta := act.Describe()
		if meta == nil {
			t.Errorf("action %T returned nil Describe()", act)
			continue
		}

		t.Run(meta.Name, func(t *testing.T) {
			if names[meta.Name] {
				t.Errorf("Duplicate action name detected: %q", meta.Name)
			}
			names[meta.Name] = true

			if typed, ok := act.(action.TypedPayload); ok {
				req := typed.ReqPayload()
				if req != nil {
					rt := reflect.TypeOf(req)
					for rt.Kind() == reflect.Pointer {
						rt = rt.Elem()
					}
					if rt.Kind() != reflect.Struct && rt.Kind() != reflect.Map {
						t.Errorf("Action %q payload must be struct or map, got %v", meta.Name, rt.Kind())
					}
				}
			}
		})
	}
}
