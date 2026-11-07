package xfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSyncParentDirectory_ReturnsOpenError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	err := syncParentDirectory(path)
	if err == nil {
		t.Fatal("syncParentDirectory succeeded for a missing directory")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error = %v, want wrapped os.ErrNotExist", err)
	}
}
