//go:build linux

package xfs_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/nexssp/kernel/xfs"
)

func TestWriteFileAtomic_RespectsUmask(t *testing.T) {
	oldUmask := syscall.Umask(0o027)
	defer syscall.Umask(oldUmask)

	path := filepath.Join(t.TempDir(), "private.txt")
	if err := xfs.WriteFileAtomic(path, []byte("secret"), 0o666); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o640); got != want {
		t.Fatalf("mode = %04o, want requested mode masked by umask: %04o", got, want)
	}
}
