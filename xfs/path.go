package xfs

import (
	"path"
	"strings"

	"github.com/nexssp/kernel/xerr"
)

// isReservedName reports whether name (already stripped of extension
// and trailing spaces) is a Win32 device name. Implemented as a
// length-dispatched EqualFold switch instead of a map: no hashing, no
// heap traffic, and the compiler can inline it into the caller's loop.
func isReservedName(name string) bool {
	switch len(name) {
	case 3:
		return strings.EqualFold(name, "con") ||
			strings.EqualFold(name, "prn") ||
			strings.EqualFold(name, "aux") ||
			strings.EqualFold(name, "nul")
	case 4:
		if strings.EqualFold(name[:3], "com") || strings.EqualFold(name[:3], "lpt") {
			return name[3] >= '0' && name[3] <= '9'
		}
		return false
	case 6:
		return strings.EqualFold(name, "conin$") ||
			strings.EqualFold(name, "clock$")
	case 7:
		return strings.EqualFold(name, "conout$")
	default:
		return false
	}
}

// Rel validates a caller-supplied path and returns its cleaned form.
func Rel(p string) (string, error) {
	if err := validateRawPath(p); err != nil {
		return "", err
	}

	clean := path.Clean(strings.ReplaceAll(p, `\`, "/"))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", xerr.Forbidden("path escapes workspace root")
	}

	for seg := range strings.SplitSeq(clean, "/") {
		if err := validateSegment(seg); err != nil {
			return "", err
		}
	}

	return clean, nil
}

func validateRawPath(p string) error {
	if p == "" {
		return xerr.BadRequest("path is required")
	}
	for i := range len(p) {
		if p[i] < 0x20 {
			return xerr.BadRequest("path must not contain control characters")
		}
	}
	if strings.IndexByte(p, ':') >= 0 {
		return xerr.Forbidden("':' is not allowed in workspace paths")
	}
	if p[0] == '/' || p[0] == '\\' {
		return xerr.Forbidden("absolute paths are not allowed")
	}
	return nil
}

func validateSegment(seg string) error {
	if strings.HasSuffix(seg, ".") {
		return xerr.Forbidden("trailing dot not allowed: " + seg)
	}
	name := seg
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		name = name[:dot]
	}
	name = strings.TrimRight(name, " ")
	if isReservedName(name) {
		return xerr.Forbidden("reserved device name: " + seg)
	}
	return nil
}
