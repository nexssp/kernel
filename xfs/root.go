package xfs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// Root confines file operations to a trusted directory. Every path
// argument is validated by Rel (fast, typed errors) and then opened
// through os.Root (kernel-level containment: symlinks cannot escape,
// TOCTOU on the resolved path is closed).
//
// Rule for the codebase: any path derived from user input, HTTP
// requests, workspace config, or plugin manifests MUST go through
// Root. xfs.WriteFile is only for paths your code builds from
// trusted constants.
type Root struct {
	r *os.Root
}

// OpenRoot opens dir as a confined root. The caller must Close it.
func OpenRoot(dir string) (*Root, error) {
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, xerr.Internal("open root", err)
	}
	return &Root{r: r}, nil
}

// Close releases the root handle.
func (r *Root) Close() error { return r.r.Close() }

// Name returns the root's directory name.
func (r *Root) Name() string { return r.r.Name() }

// FS returns an fs.FS view rooted at r's directory. Useful with
// fs.WalkDir, template.ParseFS, io/fs.ReadDir, etc.
func (r *Root) FS() fs.FS { return r.r.FS() }

// Open opens a caller-supplied path for reading.
func (r *Root) Open(p string) (*os.File, error) {
	clean, err := Rel(p)
	if err != nil {
		return nil, err
	}
	return r.r.Open(clean)
}

// ReadFile reads a caller-supplied path.
func (r *Root) ReadFile(p string) ([]byte, error) {
	clean, err := Rel(p)
	if err != nil {
		return nil, err
	}
	return r.r.ReadFile(clean)
}

// Stat returns FileInfo for a caller-supplied path.
func (r *Root) Stat(p string) (os.FileInfo, error) {
	clean, err := Rel(p)
	if err != nil {
		return nil, err
	}
	return r.r.Stat(clean)
}

// Lstat is Stat without following the final symlink.
func (r *Root) Lstat(p string) (os.FileInfo, error) {
	clean, err := Rel(p)
	if err != nil {
		return nil, err
	}
	return r.r.Lstat(clean)
}

// Create creates or truncates a file.
func (r *Root) Create(p string) (*os.File, error) {
	clean, err := Rel(p)
	if err != nil {
		return nil, err
	}
	return r.r.Create(clean)
}

// OpenFile opens with explicit flags. The path is validated by Rel; the
// kernel-level os.Root rejects symlink escapes even when O_CREATE is
// combined with a pre-existing symlink at the target.
func (r *Root) OpenFile(p string, flag int, perm os.FileMode) (*os.File, error) {
	clean, err := Rel(p)
	if err != nil {
		return nil, err
	}
	return r.r.OpenFile(clean, flag, perm)
}

// WriteFile writes data to p, confined to the root. Non-atomic: a
// crash mid-write leaves p truncated or partially written. Use only
// when the file is reconstructible (cache, generated artifact, spill
// file). For anything a reader will later trust (config, state, user
// data), use WriteFileAtomic.
func (r *Root) WriteFile(p string, data []byte, perm os.FileMode) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.WriteFile(clean, data, perm)
}

// WriteFileAtomic writes data to p atomically: a crash mid-write
// leaves p either at its previous content or at the new content, never
// in between. Costs one extra file create + fsync + rename.
//
// The temp file is created inside the root with O_EXCL so a
// pre-created file (attacker-controlled symlink, sibling race) cannot
// hijack the write. Symlink escapes are blocked by os.Root.
//
// On Windows, the final rename retries up to 5 times to survive
// transient sharing violations (AV scanners, indexers) — mirroring the
// behavior of xfs.WriteFileAtomic.
func (r *Root) WriteFileAtomic(p string, data []byte, perm os.FileMode) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}

	tmpName, f, err := r.createUniqueTemp(clean, perm)
	if err != nil {
		return err
	}

	if err := writeSyncClose(f, data); err != nil {
		r.cleanupTemp(tmpName)
		return err
	}

	if err := r.renameWithRetry(tmpName, clean); err != nil {
		r.cleanupTemp(tmpName)
		return xerr.Internal("atomic write: rename", err)
	}
	return nil
}

func (r *Root) createUniqueTemp(clean string, perm os.FileMode) (string, *os.File, error) {
	const maxAttempts = 10
	for range maxAttempts {
		suffix, err := randomHex(8)
		if err != nil {
			return "", nil, xerr.Internal("atomic write: random suffix", err)
		}
		tmpName := clean + "." + suffix + ".tmp"
		f, err := r.r.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", nil, xerr.Internal("atomic write: create tmp", err)
		}
		return tmpName, f, nil
	}
	return "", nil, xerr.Internal("atomic write: could not create unique temp after retries")
}

func writeSyncClose(f *os.File, data []byte) error {
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return xerr.Internal("atomic write: write tmp", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return xerr.Internal("atomic write: fsync tmp", err)
	}
	if err := f.Close(); err != nil {
		return xerr.Internal("atomic write: close tmp", err)
	}
	return nil
}

func (r *Root) cleanupTemp(tmpName string) {
	if rmErr := r.r.Remove(tmpName); rmErr != nil {
		slog.Warn("xfs_atomic_cleanup_failed", "tmp", tmpName, "error", rmErr)
	}
}

func (r *Root) renameWithRetry(oldName, newName string) error {
	const maxAttempts = 5
	var lastErr error
	for i := range maxAttempts {
		if lastErr = r.r.Rename(oldName, newName); lastErr == nil {
			return nil
		}
		if runtime.GOOS != "windows" {
			break
		}
		time.Sleep(time.Duration(10*(i+1)) * time.Millisecond)
	}
	return lastErr
}

// Mkdir creates a single directory.
func (r *Root) Mkdir(p string, perm os.FileMode) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.Mkdir(clean, perm)
}

// MkdirAll creates a directory tree.
func (r *Root) MkdirAll(p string, perm os.FileMode) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.MkdirAll(clean, perm)
}

// Rename moves oldp to newp, both validated by Rel.
func (r *Root) Rename(oldp, newp string) error {
	oldClean, err := Rel(oldp)
	if err != nil {
		return err
	}
	newClean, err := Rel(newp)
	if err != nil {
		return err
	}
	return r.r.Rename(oldClean, newClean)
}

// Remove removes a file or empty directory.
func (r *Root) Remove(p string) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.Remove(clean)
}

// RemoveAll removes a tree.
func (r *Root) RemoveAll(p string) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.RemoveAll(clean)
}

// Chmod changes the file mode.
func (r *Root) Chmod(p string, mode os.FileMode) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.Chmod(clean, mode)
}

// Chtimes updates access and modification times.
//
// NOTE: On Unix, Chmod/Chown/Chtimes are documented as racy — the
// stdlib may operate on a symlink if the target changes mid-call.
// Prefer writing through WriteFileAtomic over mutating in place.
func (r *Root) Chtimes(p string, atime, mtime time.Time) error {
	clean, err := Rel(p)
	if err != nil {
		return err
	}
	return r.r.Chtimes(clean, atime, mtime)
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
