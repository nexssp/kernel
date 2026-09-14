package xfs_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nexssp/kernel/xfs"
)

func TestRoot_WriteAndReadFile(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFile("file.txt", []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadFile("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestRoot_WriteAndReadFileAtomic(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.MkdirAll("sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteFileAtomic("sub/file.txt", []byte("atomic"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadFile("sub/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "atomic" {
		t.Fatalf("got %q", got)
	}
}

func TestRoot_WriteFileAtomic_OverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFileAtomic("state.json", []byte(`{"v":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteFileAtomic("state.json", []byte(`{"v":2}`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _ := r.ReadFile("state.json")
	if string(got) != `{"v":2}` {
		t.Fatalf("got %q", got)
	}
}

func TestRoot_RejectsEscapes(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, p := range []string{
		"../etc/passwd", "/etc/passwd", "..", "foo/../../bar",
		"C:/Windows", "foo:bar", "CON", "foo.", "foo\x01bar",
	} {
		if _, err := r.ReadFile(p); err == nil {
			t.Errorf("ReadFile(%q): expected rejection", p)
		}
		if err := r.WriteFile(p, []byte("x"), 0o644); err == nil {
			t.Errorf("WriteFile(%q): expected rejection", p)
		}
		if err := r.WriteFileAtomic(p, []byte("x"), 0o644); err == nil {
			t.Errorf("WriteFileAtomic(%q): expected rejection", p)
		}
	}
}

func TestRoot_SymlinkEscapeBlocked(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("top-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	r, err := xfs.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Rel accepts "link/secret.txt" as a string, but os.Root refuses to
	// follow the symlink out of the root.
	if _, err := r.ReadFile("link/secret.txt"); err == nil {
		t.Fatal("symlink escape was not blocked by os.Root")
	}
}

func TestRoot_SymlinkInsideRootAllowed(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "real.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Skipf("symlinks not supported: %v", err)
	}

	r, err := xfs.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.ReadFile("link.txt")
	if err != nil {
		t.Fatalf("in-root symlink rejected: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("got %q", got)
	}
}

func TestRoot_MkdirAllAndRemoveAll(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.MkdirAll("a/b/c", 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a", "b", "c")); err != nil {
		t.Fatalf("dir not created: %v", err)
	}
	if err := r.RemoveAll("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a")); !os.IsNotExist(err) {
		t.Fatal("dir not removed")
	}
}

func TestRoot_Rename(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFile("old.txt", []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.Rename("old.txt", "new.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadFile("old.txt"); err == nil {
		t.Fatal("old.txt still readable")
	}
	got, err := r.ReadFile("new.txt")
	if err != nil || string(got) != "data" {
		t.Fatalf("new.txt = %q, err=%v", got, err)
	}
}

func TestRoot_RenameRejectsEscape(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFile("a.txt", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.Rename("a.txt", "../escape.txt"); err == nil {
		t.Fatal("rename escaped root")
	}
}

func TestRoot_FS(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFile("f.txt", []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := r.FS().Open("f.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, 2)
	if _, err := f.Read(buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hi" {
		t.Fatalf("got %q", buf)
	}
}

func TestRoot_WriteFileAtomic_NoLeftoverTemp(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFileAtomic("data.json", []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("leftover temp file: %s", e.Name())
		}
	}
	if len(entries) != 1 || entries[0].Name() != "data.json" {
		t.Fatalf("unexpected directory contents: %v", entries)
	}
}

func TestRoot_StatAndLstat(t *testing.T) {
	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFile("file.txt", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := r.Stat("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 1 {
		t.Fatalf("size = %d", fi.Size())
	}
}

func TestRoot_Chmod(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Chmod on Windows does not set POSIX perm bits")
	}

	dir := t.TempDir()
	r, err := xfs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if err := r.WriteFile("f", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.Chmod("f", 0o600); err != nil {
		t.Fatal(err)
	}
	fi, err := r.Stat("f")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o", fi.Mode().Perm())
	}
}

func TestOpenRoot_NonexistentDir(t *testing.T) {
	_, err := xfs.OpenRoot(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("expected error opening nonexistent root")
	}
}
