package xfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/nexssp/kernel/xerr"
)

const goosWindows = "windows"

// WriteFile writes data to path non-atomically. A crash mid-write
// leaves path truncated or partially written.
//
// The caller is responsible for supplying a trusted path. WriteFile
// performs no validation — for caller-supplied (untrusted) paths,
// open an xfs.Root and use Root.WriteFile instead.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	return os.WriteFile(path, data, perm)
}

// WriteFileAtomic writes data to path atomically. A crash mid-write
// leaves path either at its previous content or at the new content;
// never in between.
//
// The caller is responsible for supplying a trusted path.
// WriteFileAtomic performs no validation — for caller-supplied
// (untrusted) paths, open an xfs.Root and use Root.WriteFileAtomic
// instead.
//
// On POSIX the rename is atomic within a filesystem. On Windows it is
// atomic within a volume and retries up to 5 times on transient
// sharing violations (a common interference on Windows hosts).
//
// data is fsync'd before the rename. The parent directory is fsync'd
// after on POSIX; Windows does not expose directory fsync and the
// rename is atomic but not durable across power loss.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)

	const maxAttempts = 10
	var tmp *os.File
	for range maxAttempts {
		suffix, err := randomHex(8)
		if err != nil {
			return xerr.Internal("atomic write: random suffix", err)
		}
		tmpPath := filepath.Join(dir, filepath.Base(path)+"."+suffix+".tmp")
		tmp, err = os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return xerr.Internal("atomic write: create tmp", err)
		}
		break
	}
	if tmp == nil {
		return xerr.Internal("atomic write: could not create unique temp after retries")
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return xerr.Internal("atomic write: write tmp", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return xerr.Internal("atomic write: fsync tmp", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return xerr.Internal("atomic write: close tmp", err)
	}
	if err := renameWithRetry(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return xerr.Internal("atomic write: rename", err)
	}

	if runtime.GOOS != goosWindows {
		if err := syncParentDirectory(dir); err != nil {
			return xerr.Internal("atomic write: sync parent directory", err)
		}
	}
	return nil
}

func syncParentDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open parent directory: %w", err)
	}
	return syncDirectory(d)
}

func syncDirectory(d *os.File) error {
	if d == nil {
		return errors.New("sync directory: nil file")
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		syncErr = fmt.Errorf("sync directory: %w", syncErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close directory: %w", closeErr)
	}
	return errors.Join(syncErr, closeErr)
}

// renameWithRetry retries on Windows for transient sharing violations.
// POSIX rename is atomic and only fails for real reasons; no retry.
func renameWithRetry(oldName, newName string) error {
	const maxAttempts = 5
	var lastErr error
	for i := range maxAttempts {
		err := os.Rename(oldName, newName)
		if err == nil {
			return nil
		}
		lastErr = err
		if runtime.GOOS != goosWindows {
			break
		}
		time.Sleep(time.Duration(10*(i+1)) * time.Millisecond)
	}
	return lastErr
}
