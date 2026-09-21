package xtest

import (
	"bytes"
	"encoding/json"
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
		tb.Fatalf("xtest.GoldenJSON: write %s: %v", path, err)
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
