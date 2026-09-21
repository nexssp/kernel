package xtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoldenJSON_CreatesThenMatches(t *testing.T) {
	t.Chdir(t.TempDir())

	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	v := payload{Name: "alice", Count: 3}

	// First call creates the file and passes.
	GoldenJSON(t, "user", v)
	if _, err := os.Stat(filepath.Join("testdata", "user.golden.json")); err != nil {
		t.Fatalf("golden file not created: %v", err)
	}

	// Second call compares and passes.
	GoldenJSON(t, "user", v)
}

func TestGoldenJSON_KeyOrderIsIgnored(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	// Write a golden with a different key order and different whitespace.
	golden := `{"b":2,"a":1}`
	if err := os.WriteFile(filepath.Join("testdata", "x.golden.json"), []byte(golden), 0o600); err != nil {
		t.Fatal(err)
	}

	GoldenJSON(t, "x", map[string]int{"a": 1, "b": 2})
}

func TestGoldenJSON_AcceptsRawJSON(t *testing.T) {
	t.Chdir(t.TempDir())

	GoldenJSON(t, "raw", []byte(`{"hello":"world"}`))
	GoldenJSON(t, "raw", []byte(`{ "hello" : "world" }`))
}

func TestGoldenJSON_MismatchFails(t *testing.T) {
	ExpectFatal(t, "TestGoldenJSON_MismatchFails", "XTEST_GOLDEN_MISMATCH",
		"mismatch",
		func(t *testing.T) {
			t.Chdir(t.TempDir())
			if err := os.MkdirAll("testdata", 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join("testdata", "y.golden.json"),
				[]byte(`{"value":1}`), 0o600); err != nil {
				t.Fatal(err)
			}
			GoldenJSON(t, "y", map[string]int{"value": 2})
		})
}

func TestGoldenJSON_UpdateRewrites(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "z.golden.json")
	if err := os.WriteFile(path, []byte(`{"value":1}`), 0o600); err != nil {
		t.Fatal(err)
	}

	GoldenJSONWithUpdate(t, "z", map[string]int{"value": 2}, true)

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"value": 2`) {
		t.Fatalf("golden not updated:\n%s", body)
	}
}

func TestGoldenJSON_UpdateOnIdenticalIsNoOp(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "same.golden.json")
	GoldenJSON(t, "same", map[string]int{"a": 1})

	info1, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	GoldenJSONWithUpdate(t, "same", map[string]int{"a": 1}, true)

	info2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info1.ModTime().Equal(info2.ModTime()) {
		t.Fatal("update rewrote an identical file")
	}
}

func TestGoldenJSON_EmptyNameFatal(t *testing.T) {
	ExpectFatal(t, "TestGoldenJSON_EmptyNameFatal", "XTEST_GOLDEN_EMPTY",
		"empty name",
		func(t *testing.T) {
			GoldenJSON(t, "", map[string]int{})
		})
}
