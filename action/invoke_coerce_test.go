package action_test

import (
	"math"
	"testing"

	"github.com/nexssp/kernel/action"
)

type coerceTarget struct {
	Count int    `json:"count"`
	Name  string `json:"name"`
}

type coerceMapKey string

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

func TestCoerce_IntegerRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   any
		want    int8
		wantErr bool
	}{
		{name: "minimum", input: -128, want: -128},
		{name: "maximum", input: 127, want: 127},
		{name: "fraction truncates within range", input: 127.9, want: 127},
		{name: "below minimum", input: -129, wantErr: true},
		{name: "above maximum", input: 128, wantErr: true},
		{name: "narrowing to zero", input: 256, wantErr: true},
		{name: "float above maximum", input: 256.0, wantErr: true},
		{name: "NaN", input: math.NaN(), wantErr: true},
		{name: "positive infinity", input: math.Inf(1), wantErr: true},
		{name: "negative infinity", input: math.Inf(-1), wantErr: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := action.Coerce[int8](test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("Coerce[int8](%#v) error = %v, wantErr %v", test.input, err, test.wantErr)
			}
			if err == nil && got != test.want {
				t.Fatalf("Coerce[int8](%#v) = %d, want %d", test.input, got, test.want)
			}
		})
	}

	if _, err := action.Coerce[uint8](-1); err == nil {
		t.Fatal("Coerce[uint8](-1) succeeded")
	}
	if _, err := action.Coerce[uint8](256); err == nil {
		t.Fatal("Coerce[uint8](256) succeeded")
	}
	if _, err := action.Coerce[int64](uint64(1) << 63); err == nil {
		t.Fatal("Coerce[int64](uint64(1) << 63) succeeded")
	}
}

func FuzzCoerceInt8Range(f *testing.F) {
	for _, seed := range []int64{-129, -128, -1, 0, 127, 128, 256} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input int64) {
		got, err := action.Coerce[int8](input)
		if input < -128 || input > 127 {
			if err == nil {
				t.Fatalf("Coerce[int8](%d) = %d, want overflow error", input, got)
			}
			return
		}
		if err != nil || int64(got) != input {
			t.Fatalf("Coerce[int8](%d) = %d, %v; want exact value", input, got, err)
		}
	})
}

func BenchmarkCoerceInt8(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		if _, err := action.Coerce[int8](127); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCoerce_MapWithNamedStringKeys(t *testing.T) {
	t.Run("named source key to builtin target key", func(t *testing.T) {
		got, err := action.Coerce[map[string]int](map[coerceMapKey]any{"a": 1})
		if err != nil {
			t.Fatalf("Coerce returned error: %v", err)
		}
		if got["a"] != 1 {
			t.Fatalf("Coerce returned %#v, want map[string]int{\"a\": 1}", got)
		}
	})

	t.Run("builtin source key to named target key", func(t *testing.T) {
		got, err := action.Coerce[map[coerceMapKey]int](map[string]any{"a": 1})
		if err != nil {
			t.Fatalf("Coerce returned error: %v", err)
		}
		if got[coerceMapKey("a")] != 1 {
			t.Fatalf("Coerce returned %#v, want map[coerceMapKey]int{\"a\": 1}", got)
		}
	})
}
