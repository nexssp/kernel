package secret_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/nexssp/kernel/secret"
)

func TestValueZeroValueAndScopedAccess(t *testing.T) {
	var empty secret.Value
	if !secret.IsEmpty(empty) {
		t.Fatal("zero value should be empty")
	}
	if err := secret.WithValue(empty, func(value []byte) error {
		if len(value) != 0 {
			t.Fatalf("zero-value bytes = %q, want empty", value)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithValue(zero): %v", err)
	}

	value := secret.New("scope-canary")
	if secret.IsEmpty(value) {
		t.Fatal("constructed value should not be empty")
	}
	var retained []byte
	if err := secret.WithValue(value, func(raw []byte) error {
		if string(raw) != "scope-canary" {
			t.Fatalf("callback bytes = %q", raw)
		}
		retained = raw
		return nil
	}); err != nil {
		t.Fatalf("WithValue: %v", err)
	}
	if !bytes.Equal(retained, make([]byte, len("scope-canary"))) {
		t.Fatalf("callback copy was not cleared: %q", retained)
	}
	if err := secret.WithValue(value, func(raw []byte) error {
		if string(raw) != "scope-canary" {
			t.Fatalf("clearing callback copy changed the value: %q", raw)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithValue after clear: %v", err)
	}
}

func TestWithValueRequiresCallback(t *testing.T) {
	if err := secret.WithValue(secret.New("hidden"), nil); !errors.Is(err, secret.ErrNilCallback) {
		t.Fatalf("WithValue(nil) error = %v, want ErrNilCallback", err)
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{name: "equal", a: "same-secret", b: "same-secret", want: true},
		{name: "same length, different contents", a: "secret-one", b: "secret-two", want: false},
		{name: "different lengths", a: "short", b: "longer-secret", want: false},
		{name: "empty", a: "", b: "", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := secret.Equal(secret.New(test.a), secret.New(test.b)); got != test.want {
				t.Fatalf("Equal() = %t, want %t", got, test.want)
			}
		})
	}
}

var equalBenchmarkSink bool

func BenchmarkEqual(b *testing.B) {
	left := secret.New("benchmark-secret-value")
	right := secret.New("benchmark-secret-value")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		equalBenchmarkSink = secret.Equal(left, right)
	}
}

func TestGobEncodingFailsClosed(t *testing.T) {
	value := secret.New("gob-canary")
	encoded, err := value.GobEncode()
	if !errors.Is(err, secret.ErrCannotSerialize) {
		t.Fatalf("GobEncode error = %v, want ErrCannotSerialize", err)
	}
	if encoded != nil {
		t.Fatalf("GobEncode returned %d bytes on error", len(encoded))
	}
	var wire bytes.Buffer
	if err := gob.NewEncoder(&wire).Encode(value); !errors.Is(err, secret.ErrCannotSerialize) {
		t.Fatalf("gob.Encode error = %v, want ErrCannotSerialize", err)
	}
}

func TestReflectDeepEqualStillInspectsSecretValue(t *testing.T) {
	// reflect.DeepEqual can inspect the private field and is not constant-time.
	// This test documents that callers must use secret.Equal instead.
	left := secret.New("same-secret")
	right := secret.New("same-secret")
	if !reflect.DeepEqual(left, right) {
		t.Fatal("reflect.DeepEqual should still compare Value contents")
	}
}

func TestValueRedactsFormattingSerializationAndLogs(t *testing.T) {
	const canary = "value-canary-never-output"
	value := secret.New(canary)
	for _, format := range []string{"%s", "%q", "%+v", "%#v", "%x"} {
		got := fmt.Sprintf(format, value)
		if strings.Contains(got, canary) || !strings.Contains(got, secret.Redacted) {
			t.Errorf("fmt.Sprintf(%q, value) = %q", format, got)
		}
	}
	encoded, err := json.Marshal(struct {
		Key secret.Value `json:"key"`
	}{Key: value})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(encoded), canary) || !strings.Contains(string(encoded), secret.Redacted) {
		t.Fatalf("JSON output = %s", encoded)
	}
	text, err := value.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	if string(text) != secret.Redacted {
		t.Fatalf("text output = %q", text)
	}
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("credential", "value", value)
	if strings.Contains(logs.String(), canary) || !strings.Contains(logs.String(), secret.Redacted) {
		t.Fatalf("slog output = %s", logs.String())
	}
}

func TestValueCannotBeRestoredFromSerialization(t *testing.T) {
	value := secret.New("existing")
	if err := json.Unmarshal([]byte(`"replacement"`), &value); !errors.Is(err, secret.ErrCannotDeserialize) {
		t.Fatalf("json.Unmarshal error = %v, want ErrCannotDeserialize", err)
	}
	if err := value.UnmarshalText([]byte("replacement")); !errors.Is(err, secret.ErrCannotDeserialize) {
		t.Fatalf("UnmarshalText error = %v, want ErrCannotDeserialize", err)
	}
	if err := value.GobDecode([]byte("replacement")); !errors.Is(err, secret.ErrCannotDeserialize) {
		t.Fatalf("GobDecode error = %v, want ErrCannotDeserialize", err)
	}
	if err := secret.WithValue(value, func(raw []byte) error {
		if string(raw) != "existing" {
			t.Fatalf("failed unmarshal changed value: %q", raw)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithValue: %v", err)
	}
}

func TestValueIsOpaqueNonStringType(t *testing.T) {
	valueType := reflect.TypeFor[secret.Value]()
	if valueType.Kind() == reflect.String || valueType.ConvertibleTo(reflect.TypeFor[string]()) {
		t.Fatalf("Value type %v must not be string-convertible", valueType)
	}
	if valueType.Comparable() {
		t.Fatalf("Value type %v must not be comparable", valueType)
	}
}
