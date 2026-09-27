package action_test

import (
	"testing"

	"github.com/nexssp/kernel/action"
)

type coerceTarget struct {
	Count int    `json:"count"`
	Name  string `json:"name"`
}

func TestCoerce_StrictOnFailedFieldAssignment(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"valid_map", map[string]any{"count": 3, "name": "x"}, false},
		{"valid_map_with_extra", map[string]any{"count": 3, "name": "x", "unknown": true}, false},
		{"bad_count_string", map[string]any{"count": "abc", "name": "x"}, true},
		{"bad_count_slice", map[string]any{"count": []int{1}, "name": "x"}, true},
		{"bad_name_map", map[string]any{"count": 1, "name": map[string]any{}}, true},
		{"valid_typed", coerceTarget{Count: 5, Name: "y"}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			_, err := action.Coerce[coerceTarget](c.input)
			if (err != nil) != c.wantErr {
				t.Fatalf("input %#v: err = %v, wantErr = %v", c.input, err, c.wantErr)
			}
		})
	}
}

func TestCoerce_StrictSlice(t *testing.T) {
	t.Parallel()

	if _, err := action.Coerce[[]int]([]any{1, 2, 3}); err != nil {
		t.Fatalf("valid slice: %v", err)
	}
	if _, err := action.Coerce[[]int]([]any{1, "two", 3}); err == nil {
		t.Fatal("expected error for []any{1, \"two\", 3} into []int")
	}
}

func TestCoerce_StrictMap(t *testing.T) {
	t.Parallel()

	if _, err := action.Coerce[map[string]int](map[string]any{"a": 1, "b": 2}); err != nil {
		t.Fatalf("valid map: %v", err)
	}
	if _, err := action.Coerce[map[string]int](map[string]any{"a": 1, "b": "two"}); err == nil {
		t.Fatal("expected error for mixed-type values into map[string]int")
	}
}
