package action

import (
	"errors"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Named types verify that Convert(target) preserves the declared type
// instead of returning the underlying builtin.
type (
	Port     int
	Timeout  time.Duration // derived from Duration: documented as int64-only
	Label    string
	Ratio    float32
	Bytes    []byte
	Byte     byte
	ByteList []Byte
	Strings  []string
)

func typeOf[T any]() reflect.Type { return reflect.TypeFor[T]() }

func TestCoerceStringValue(t *testing.T) {
	t.Run("happy paths", func(t *testing.T) {
		tests := []struct {
			name   string
			raw    string
			target reflect.Type
			want   any
		}{
			// string: raw preserved verbatim, including whitespace.
			{"string", "hello", typeOf[string](), "hello"},
			{"string keeps whitespace", "  padded  ", typeOf[string](), "  padded  "},
			{"string empty", "", typeOf[string](), ""},
			{"named string", "x", typeOf[Label](), Label("x")},

			// bool.
			{"bool true", "true", typeOf[bool](), true},
			{"bool false", "0", typeOf[bool](), false},
			{"bool trims whitespace", "  t  ", typeOf[bool](), true},

			// ints: exact width, negative, named type.
			{"int", "42", typeOf[int](), 42},
			{"int8 min", "-128", typeOf[int8](), int8(-128)},
			{"int16", "32000", typeOf[int16](), int16(32000)},
			{"int64", "-9223372036854775808", typeOf[int64](), int64(-9223372036854775808)},
			{"int trims whitespace", "  7 ", typeOf[int](), 7},
			{"named int keeps type", "8080", typeOf[Port](), Port(8080)},

			// uints.
			{"uint", "42", typeOf[uint](), uint(42)},
			{"uint8 max", "255", typeOf[uint8](), uint8(255)},
			{"uint64 max", "18446744073709551615", typeOf[uint64](), uint64(18446744073709551615)},

			// floats: named float32 stays float32.
			{"float64", "3.14", typeOf[float64](), 3.14},
			{"float32 named", "2.5", typeOf[Ratio](), Ratio(2.5)},
			{"float scientific", "1e3", typeOf[float64](), 1000.0},

			// time.Duration: Go syntax; derived named type takes integer.
			{"duration", "1m30s", typeOf[time.Duration](), 90 * time.Second},
			{"duration trims whitespace", " 5s ", typeOf[time.Duration](), 5 * time.Second},
			{"derived duration as int64", "5000000000", typeOf[Timeout](), Timeout(5 * time.Second)},

			// []byte verbatim; named byte slices keep their declared type.
			{"bytes verbatim", "Hello, World", typeOf[[]byte](), []byte("Hello, World")},
			{"named bytes", "abc", typeOf[Bytes](), Bytes("abc")},
			{"slice of named byte", "raw", typeOf[ByteList](), ByteList("raw")},

			// slices: CSV splitting, trimming, empty-element skipping.
			{"string slice", "a,b,c", typeOf[[]string](), []string{"a", "b", "c"}},
			{"empty string yields empty slice", "", typeOf[[]string](), []string{}},
			{"trailing comma skipped", "a,b,", typeOf[[]string](), []string{"a", "b"}},
			{"inner empty skipped", "a,,b", typeOf[[]string](), []string{"a", "b"}},
			{"elements trimmed", " 1 , 2 ,3 ", typeOf[[]int](), []int{1, 2, 3}},
			{"int slice", "1,2,3", typeOf[[]int](), []int{1, 2, 3}},
			{"named slice type", "x,y", typeOf[Strings](), Strings{"x", "y"}},
			{"duration slice", "1s,2s", typeOf[[]time.Duration](), []time.Duration{time.Second, 2 * time.Second}},
			{"slice of pointers", "1,2", typeOf[[]*int](), ptrSlice(1, 2)},

			// pointers: allocated via new(expr) (modernize).
			{"pointer to int", "42", typeOf[*int](), new(42)},
			{"double pointer", "7", typeOf[**int](), new(new(7))},
			{"pointer to duration", "3s", typeOf[*time.Duration](), new(3 * time.Second)},
			{"pointer to bytes", "hi", typeOf[*[]byte](), new([]byte("hi"))},

			{"time.Time", "2026-01-15T10:30:00Z", typeOf[time.Time](), time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)},
			{"net.IP", "192.168.1.1", typeOf[net.IP](), net.ParseIP("192.168.1.1")},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := CoerceStringValue(tt.raw, tt.target)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("got %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
				}
			})
		}
	})

	t.Run("errors", func(t *testing.T) {
		tests := []struct {
			name    string
			raw     string
			target  reflect.Type
			wantMsg string // substring expected in the error
		}{
			{"nil target", "x", nil, "nil target"},
			{"int8 overflow", "300", typeOf[int8](), "out of range"},
			{"uint rejects negative", "-1", typeOf[uint](), "invalid syntax"},
			{"bad float", "abc", typeOf[float64](), "invalid syntax"},
			{"bad bool", "yes", typeOf[bool](), "invalid syntax"},
			{"bad duration", "10", typeOf[time.Duration](), "missing unit"},
			{"derived duration rejects syntax", "5s", typeOf[Timeout](), "invalid syntax"},
			{"map unsupported", "x", typeOf[map[string]string](), "unsupported kind"},
			{"struct unsupported", "x", typeOf[struct{ A int }](), "unsupported kind"},
			{"slice element error carries index", "1,x,3", typeOf[[]int](), "element 1"},
			{"error names target type", "abc", typeOf[int16](), "int16"},
			{"time.Time invalid", "not-a-time", typeOf[time.Time](), "cannot parse"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := CoerceStringValue(tt.raw, tt.target)
				if err == nil {
					t.Fatalf("expected error, got value %#v", got)
				}
				if !strings.Contains(err.Error(), tt.wantMsg) {
					t.Errorf("error %q does not contain %q", err, tt.wantMsg)
				}
			})
		}
	})

	t.Run("result is directly assignable to struct field", func(t *testing.T) {
		type Config struct {
			Name    string
			Port    Port
			Rate    Ratio
			Timeout time.Duration
			Tags    []string
			Data    Bytes
			Retry   *int
		}

		inputs := map[string]string{
			"Name":    "svc",
			"Port":    "8080",
			"Rate":    "0.75",
			"Timeout": "250ms",
			"Tags":    "a,b",
			"Data":    "blob",
			"Retry":   "3",
		}

		var cfg Config
		v := reflect.ValueOf(&cfg).Elem()
		for name, raw := range inputs {
			field := v.FieldByName(name)
			got, err := CoerceStringValue(raw, field.Type())
			if err != nil {
				t.Fatalf("field %s: %v", name, err)
			}
			field.Set(reflect.ValueOf(got))
		}

		want := Config{
			Name:    "svc",
			Port:    8080,
			Rate:    0.75,
			Timeout: 250 * time.Millisecond,
			Tags:    []string{"a", "b"},
			Data:    Bytes("blob"),
			Retry:   new(3),
		}
		if !reflect.DeepEqual(cfg, want) {
			t.Errorf("got %+v, want %+v", cfg, want)
		}
	})

	t.Run("error unwraps to strconv.NumError", func(t *testing.T) {
		_, err := CoerceStringValue("nope", typeOf[int]())
		if _, ok := errors.AsType[*strconv.NumError](err); !ok {
			t.Errorf("expected wrapped *strconv.NumError, got %T", err)
		}
	})
}

func ptrSlice[T any](vs ...T) []*T {
	out := make([]*T, len(vs))
	for i := range vs {
		out[i] = new(vs[i])
	}
	return out
}
