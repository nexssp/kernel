package xtest

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var UpdateGolden = flag.Bool("xtest.update", false, "rewrite golden files")

func GoldenJSON(tb testing.TB, name string, got any) {
	tb.Helper()
	goldenJSON(tb, name, got, *UpdateGolden)
}

func GoldenJSONWithUpdate(tb testing.TB, name string, got any, update bool) {
	tb.Helper()
	goldenJSON(tb, name, got, update)
}

func goldenJSON(tb testing.TB, name string, got any, update bool) {
	tb.Helper()
	if name == "" {
		tb.Fatal("xtest.GoldenJSON: empty name")
	}

	path := filepath.Join("testdata", name+".golden.json")

	gotCanon, err := canonicalJSON(got)
	if err != nil {
		tb.Fatalf("xtest.GoldenJSON: canonicalize %T: %v", got, err)
	}
	if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
		tb.Fatalf("xtest.GoldenJSON: mkdir: %v", mkErr)
	}

	existing, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		tb.Fatalf("xtest.GoldenJSON: read %s: %v", path, readErr)
	}
	if readErr == nil && bytes.Equal(existing, gotCanon) {
		return
	}

	if readErr != nil {
		writeGolden(tb, path, gotCanon)
		tb.Logf("xtest.GoldenJSON: created initial golden file %s", path)
		return
	}

	existingCanon, err := canonicalJSON(existing)
	if err != nil {
		tb.Fatalf("xtest.GoldenJSON: canonicalize golden %s: %v", path, err)
	}

	if bytes.Equal(existingCanon, gotCanon) {
		return
	}

	if update {
		writeGolden(tb, path, gotCanon)
		tb.Logf("xtest.GoldenJSON: updated %s", path)
		return
	}

	tb.Fatalf("xtest.GoldenJSON: %s mismatch\n  want: %s\n  got:  %s\n(run with -xtest.update to rewrite)",
		path, existingCanon, gotCanon)
}

func writeGolden(tb testing.TB, path string, data []byte) {
	tb.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		tb.Fatalf("xtest: write golden %s: %v", path, err)
	}
}

func canonicalJSON(v any) ([]byte, error) {
	var raw []byte
	switch x := v.(type) {
	case []byte:
		raw = x
	case json.RawMessage:
		raw = x
	default:
		var err error
		raw, err = json.Marshal(v)
		if err != nil {
			return nil, err
		}
	}

	var pretty any
	if err := json.Unmarshal(raw, &pretty); err != nil {
		return nil, err
	}

	out, err := json.MarshalIndent(pretty, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

// GoldenText compares got against testdata/<name>.golden. The
// -xtest.update flag rewrites the file.
func GoldenText(tb testing.TB, name, got string) {
	tb.Helper()
	GoldenTextWithUpdate(tb, name, got, *UpdateGolden)
}

func GoldenTextWithUpdate(tb testing.TB, name, got string, update bool) {
	tb.Helper()
	if name == "" {
		tb.Fatal("xtest.GoldenText: empty name")
	}

	path := filepath.Join("testdata", name+".golden")

	if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
		tb.Fatalf("xtest.GoldenText: mkdir: %v", mkErr)
	}

	existing, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		tb.Fatalf("xtest.GoldenText: read %s: %v", path, readErr)
	}

	gotBytes := []byte(got)
	if readErr == nil && bytes.Equal(existing, gotBytes) {
		return
	}

	if readErr != nil {
		writeGolden(tb, path, gotBytes)
		tb.Logf("xtest.GoldenText: created initial golden file %s", path)
		return
	}

	if update {
		writeGolden(tb, path, gotBytes)
		tb.Logf("xtest.GoldenText: updated %s", path)
		return
	}

	tb.Fatalf("xtest.GoldenText: %s mismatch\n--- want ---\n%s\n--- got ---\n%s\n(run with -xtest.update to rewrite)",
		path, string(existing), got)
}
