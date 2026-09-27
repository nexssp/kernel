package action

import "testing"

func TestIsTruthyAny(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  any
		want bool
	}{
		{"nil", nil, false},

		{"bool_true", true, true},
		{"bool_false", false, false},

		{"int_zero", 0, false},
		{"int_one", 1, true},
		{"int_negative", -1, true},

		{"int64_zero", int64(0), false},
		{"int64_one", int64(1), true},

		{"float64_zero", 0.0, false},
		{"float64_half", 0.5, true},
		{"float64_negative", -1.0, true},

		{"string_empty", "", false},
		{"string_false", "false", false},
		{"string_no", "no", false},
		{"string_zero", "0", false},
		{"string_true", "true", true},
		{"string_yes", "yes", true},
		{"string_word", "hello", true},

		{"map_empty", map[string]any{}, false},
		{"map_single_true", map[string]any{"a": true}, true},
		{"map_single_false", map[string]any{"a": false}, false},
		{"map_single_zero", map[string]any{"a": 0}, false},
		{"map_single_one", map[string]any{"a": 1}, true},
		{"map_single_nil", map[string]any{"a": nil}, false},
		{"map_single_empty_string", map[string]any{"a": ""}, false},

		{"map_approved_false", map[string]any{"approved": false}, false},
		{"map_approved_true", map[string]any{"approved": true}, true},
		{"map_match_false", map[string]any{"match": false}, false},
		{"map_match_true", map[string]any{"match": true}, true},
		{"map_passed_false", map[string]any{"passed": false}, false},
		{"map_ok_false", map[string]any{"ok": false}, false},

		{"map_mixed_one_truthy", map[string]any{"a": false, "b": true}, true},
		{"map_mixed_all_falsy", map[string]any{"a": false, "b": 0}, false},
		{"map_mixed_string", map[string]any{"a": "", "b": "x"}, true},

		{"map_nested_truthy", map[string]any{"outer": map[string]any{"inner": true}}, true},
		{"map_nested_falsy", map[string]any{"outer": map[string]any{"inner": false}}, false},
		{"map_nested_empty", map[string]any{"outer": map[string]any{}}, false},

		{"slice_nonempty", []any{1, 2}, true},
		{"struct_zero_value", struct{ X int }{}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := isTruthyAny(c.val); got != c.want {
				t.Fatalf("isTruthyAny(%#v) = %v, want %v", c.val, got, c.want)
			}
		})
	}
}
