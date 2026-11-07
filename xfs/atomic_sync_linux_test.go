//go:build linux

package xfs

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestSyncDirectory_ReturnsSyncError(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	err = syncDirectory(writer)
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("syncDirectory error = %v, want wrapped EINVAL for pipe fsync", err)
	}
}
