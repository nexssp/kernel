package xfs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nexssp/kernel/xfs"
)

func TestWriteFileAtomic_WritesNewContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	if err := xfs.WriteFileAtomic(path, []byte(`{"version":1}`), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := xfs.WriteFileAtomic(path, []byte(`{"version":2}`), 0o640); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"version":2}` {
		t.Fatalf("content = %q, want latest atomic write", got)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		t.Fatalf("unexpected directory contents: %v", entries)
	}
}
