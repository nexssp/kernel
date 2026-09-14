package xfs_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/nexssp/kernel/xfs"
)

func TestRel_HappyPaths(t *testing.T) {
	cases := map[string]string{
		"foo":          "foo",
		"./foo":        "foo",
		"foo/bar":      "foo/bar",
		"foo/./bar":    "foo/bar",
		"foo//bar":     "foo/bar",
		"a/b/../c":     "a/c",
		"a/b/../../a":  "a",
		"foo\\bar":     "foo/bar",
		"foo\\bar/baz": "foo/bar/baz",
		"..foo":        "..foo",
		"foo..bar":     "foo..bar",
		"foo bar":      "foo bar",
		"a.b/c.d":      "a.b/c.d",
		"123/456":      "123/456",
	}
	for in, want := range cases {
		got, err := xfs.Rel(in)
		if err != nil {
			t.Errorf("Rel(%q): unexpected error %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("Rel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRel_EmptyAndCurrentDir(t *testing.T) {
	for _, in := range []string{"", ".", "./"} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): expected error", in)
		}
	}
}

func TestRel_NULBytes(t *testing.T) {
	for _, in := range []string{"\x00", "\x00foo", "foo\x00", "foo\x00bar"} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): NUL should be rejected", in)
		}
	}
}

func TestRel_ControlChars(t *testing.T) {
	for _, in := range []string{
		"\x01", "\x1f", "\x1f\x1f",
		"foo\x01bar", "foo\tbar", "foo\nbar", "foo\rbar",
		"a/b\x1f", "a\x02/b", "foo\x7fbar", // DEL is 0x7f, allowed per current contract
	} {
		if in == "foo\x7fbar" {
			continue // DEL is not < 0x20; skip the reject expectation
		}
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): control char should be rejected", in)
		}
	}
}

func TestRel_AbsolutePaths(t *testing.T) {
	for _, in := range []string{
		"/", "/etc/passwd", "/foo", "//foo", "///foo",
		`\`, `\etc\passwd`, `\\server\share`,
		`\\?\C:\Windows`, `\\.\PhysicalDrive0`,
	} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): absolute path should be rejected", in)
		}
	}
}

func TestRel_Traversal(t *testing.T) {
	for _, in := range []string{
		"..", "../", "./../foo", "../foo", "../../etc/passwd",
		"foo/../..", "foo/../../bar", "a/b/c/../../../..",
		`..\x`, `..\..\x`, `foo\..\..\x`, `..\..//..`,
		"foo/..", "foo/../bar/..", "foo/../../foo",
	} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): traversal should be rejected", in)
		}
	}
}

func TestRel_WindowsADS(t *testing.T) {
	for _, in := range []string{
		":", "::", "foo:bar", "foo.txt:secret", "foo.txt:$DATA",
		"foo:bar:baz", "a/b:secret", "a/b/c:d",
		"file.log:Zone.Identifier",
		"C:", "c:", "C:foo", "C:/foo", `C:\foo`, "C:/../../x",
	} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): ADS/drive should be rejected", in)
		}
	}
}

func TestRel_WindowsReservedNames(t *testing.T) {
	// Every reserved name × every variation that Win32 treats the same:
	// case, trailing dot, trailing space, extension, nested path.
	for _, in := range []string{
		// Case variants for CON.
		"CON", "con", "Con", "cOn",
		"PRN", "prn", "AUX", "aux", "Aux", "NUL", "nul", "Nul",
		// Case variants for COM.
		"COM0", "com0", "COM1", "com1", "Com1", "COM9", "com9",
		// Case variants for LPT.
		"LPT0", "lpt0", "LPT1", "lpt1", "Lpt1", "LPT9", "lpt9",
		// Documented but commonly omitted.
		"CONIN$", "conin$", "CONOUT$", "conout$",
		"CLOCK$", "clock$", "Clock$",
		// Extensions.
		"CON.txt", "CON.log", "CON.foo.bar", "con.txt",
		"NUL.txt", "NUL.log", "AUX.log", "COM1.txt", "LPT9.txt",
		// Trailing spaces (Win32 strips before resolution).
		"CON ", "NUL ", "COM1 ", "LPT9 ", "AUX ", "PRN ",
		// Reserved name inside a path.
		"foo/CON", "foo/CON/bar", "a/b/NUL.txt", "dir/COM1/file",
		"foo/CON ", "a/NUL ",
		// Windows separator before normalization.
		`foo\CON`, `CON\bar`,
	} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): reserved name should be rejected", in)
		}
	}
}

func TestRel_NotReserved(t *testing.T) {
	// Lookalikes that must NOT be rejected. If any of these fails, the
	// reserved-name matching is too aggressive.
	for _, in := range []string{
		// Prefix but not exact.
		"CONSOLE", "console", "CONN", "CON_", "CON-",
		// Sibling names that are reserved-adjacent.
		"NULL", "nulls", "null_", "_null",
		"COM", "COM10", "COM99", "COM_",
		"LPT", "LPT10", "LPT99", "LPT_",
		"PRNT", "PRN_", "PRN-",
		"AUXILIARY", "AUXILIAR", "AUX_",
		// Prefix variations.
		"mycon", "precon", "suffixcon", "com1x", "lpt1y",
		// Device-name lookalikes missing the $.
		"CONIN", "CONOUT", "CON$",
		// Suffixes that look reserved but aren't.
		"con-", "con_",
	} {
		if _, err := xfs.Rel(in); err != nil {
			t.Errorf("Rel(%q): should be allowed, got %v", in, err)
		}
	}
}

func TestRel_TrailingDot(t *testing.T) {
	// Trailing dots are rejected because Win32 strips them before
	// resolution: "foo." and "foo" collide on Windows, and "..." resolves
	// to an empty base name. Rejecting keeps Linux and Windows semantics
	// identical.
	for _, in := range []string{
		"foo.", "foo..", "a/b.", "a./b", "file.txt.", "..foo.",
		"foo./bar", "a/b./c",
		"foo/.._../bar",
	} {
		if _, err := xfs.Rel(in); err == nil {
			t.Errorf("Rel(%q): trailing dot should be rejected", in)
		}
	}
}

func TestRel_TrailingSpace(t *testing.T) {
	// Trailing spaces are legal on Linux and Win32 accepts them for the
	// reserved-name check only. Non-reserved names keep the space in the
	// returned path.
	if got, err := xfs.Rel("foo "); err != nil || got != "foo " {
		t.Errorf("Rel(%q) = %q, %v; want \"foo \"", "foo ", got, err)
	}
}

func TestRel_UnicodeAndFullwidth(t *testing.T) {
	// Non-ASCII names must be preserved.
	if got, err := xfs.Rel("文件/データ.txt"); err != nil || got != "文件/データ.txt" {
		t.Errorf("unicode path: got %q, err %v", got, err)
	}
	// Fullwidth solidus is NOT a path separator; it's a regular char.
	if got, err := xfs.Rel("foo／bar"); err != nil || got != "foo／bar" {
		t.Errorf("fullwidth solidus: got %q, err %v", got, err)
	}
}

func TestRel_EncodedSlashIsNotSeparator(t *testing.T) {
	// %2F is the URL-encoded slash as a literal three-character
	// sequence '%','2','f' — NOT an actual '/' byte. It must not be
	// treated as a path separator. `\x2f` is deliberately excluded
	// here: in a Go string literal it decodes to the real '/' byte
	// (0x2F), so it *is* a separator and belongs in the happy-path
	// tests, not here.
	for _, in := range []string{"foo%2fbar", "foo%2F..%2Fbar"} {
		got, err := xfs.Rel(in)
		if err != nil {
			t.Errorf("Rel(%q): should be allowed: %v", in, err)
			continue
		}
		if strings.Contains(got, "/") {
			t.Errorf("Rel(%q) = %q: unexpected segment split", in, got)
		}
	}
}

func TestRel_JoinedPathStaysInsideRoot(t *testing.T) {
	root := filepath.Join("tmp", "root")
	for _, in := range []string{
		"foo", "foo/bar", "a/b/../c", `foo\bar`,
		"deeply/nested/path/to/file.txt",
	} {
		clean, err := xfs.Rel(in)
		if err != nil {
			t.Fatalf("Rel(%q): %v", in, err)
		}
		full := filepath.Join(root, clean)
		if !strings.HasPrefix(full, root+string(filepath.Separator)) {
			t.Fatalf("Rel(%q)=%q joins to %q, escapes %q", in, clean, full, root)
		}
	}
}

// FuzzRel asserts the postcondition that makes Rel's output safe to
// join with a trusted root. Any input that survives the validator must
// satisfy every invariant below.
func FuzzRel(f *testing.F) {
	seeds := []string{
		// ── Happy paths ────────────────────────────────────────────────
		"foo", "foo/bar", "foo/bar/baz",
		"a.b/c.d", "..foo", "foo..bar", "foo bar",
		"foo_bar-baz", "123/456", "Ünïcödé/ファイル",
		"foo/./bar", "foo//bar",
		"a/b/../c", "a/b/../../a",
		"foo\\bar", "foo\\bar/baz",
		"deeply/nested/path/to/file.txt",

		// ── Empty / NUL / control ─────────────────────────────────────
		"", ".", "./", "./foo",
		"\x00", "\x00foo", "foo\x00", "foo\x00bar",
		"\x01\x02\x03", "foo\tbar", "foo\nbar",

		// ── Absolute (POSIX) ──────────────────────────────────────────
		"/", "//", "///", "/foo", "/etc/passwd",

		// ── Absolute (Windows) ────────────────────────────────────────
		`\`, `\\`, `\\\`, `\foo`, `\etc\passwd`,
		`\\server\share`, `\\?\C:\Windows`, `\\.\PhysicalDrive0`,

		// ── Windows drive ─────────────────────────────────────────────
		"C:", "c:", "C:foo", "C:/foo", `C:\foo`, "C:/../../x", "AB:foo",

		// ── Windows ADS ───────────────────────────────────────────────
		":", "::", "foo:bar", "foo.txt:secret", "foo.txt:$DATA",
		"foo:bar:baz", "a/b:secret", "a/b/c:d",
		"file.log:Zone.Identifier",

		// ── Traversal ─────────────────────────────────────────────────
		"..", "../", "./../foo", "../foo",
		"../../etc/passwd", "foo/../..", "foo/../../bar",
		"a/b/c/../../../..",
		`..\x`, `..\..\x`, `foo\..\..\x`, `..\..//..`,
		"foo/..", "foo/../bar", "foo/../bar/..",
		"foo/../../foo", "....//....//etc",

		// ── Reserved names ────────────────────────────────────────────
		"CON", "con", "Con", "cOn",
		"PRN", "prn", "AUX", "aux", "Aux", "NUL", "nul", "Nul",
		"COM0", "com0", "COM1", "com1", "COM9", "com9",
		"LPT0", "lpt0", "LPT1", "lpt1", "LPT9", "lpt9",
		"CONIN$", "CONOUT$", "CLOCK$",
		"CON.txt", "NUL.txt", "AUX.log", "COM1.txt", "LPT9.txt",
		"CON ", "NUL ", "LPT9 ",
		"foo/CON", "foo/CON/bar", "a/b/NUL.txt",
		`foo\CON`, `CON\bar`,

		// ── Trailing dot ──────────────────────────────────────────────
		"foo.", "foo..", "a/b.", "a./b", "file.txt.", "..foo.",
		"foo./bar", "a/b./c",

		// ── Lookalikes that must not be rejected ──────────────────────
		"CONSOLE", "console", "NULL", "nulls",
		"COM", "COM10", "COM99", "LPT", "LPT10", "LPT99",
		"PRNT", "AUXILIARY", "CONIN", "CONOUT",
		"mycon", "precon", "suffixcon",
		"con_", "con-",

		// ── Length / boundary ─────────────────────────────────────────
		strings.Repeat("a", 255),
		strings.Repeat("a", 4096),
		strings.Repeat("a/", 100) + "b",
		strings.Repeat("../", 100) + "x",

		// ── Separators ────────────────────────────────────────────────
		"foo/", "foo/bar/", "foo/bar/./", "foo/bar/../",
		"foo\x2fbar", "foo%2fbar", "foo\uff0fbar",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, p string) {
		clean, err := xfs.Rel(p)
		if err != nil {
			return
		}
		// Postcondition: the result must be safe to filepath.Join with a
		// trusted root.
		if clean == "" {
			t.Fatalf("Rel(%q) returned empty", p)
		}
		if clean == "." || clean == ".." {
			t.Fatalf("Rel(%q) returned %q", p, clean)
		}
		if strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, `\`) {
			t.Fatalf("Rel(%q) returned absolute %q", p, clean)
		}
		if strings.HasPrefix(clean, "../") {
			t.Fatalf("Rel(%q) returned traversal %q", p, clean)
		}
		if strings.IndexByte(clean, ':') >= 0 {
			t.Fatalf("Rel(%q) returned colon %q", p, clean)
		}
		for _, c := range []byte(clean) {
			if c < 0x20 {
				t.Fatalf("Rel(%q) returned control char %#x", p, c)
			}
		}
		// Reserved device names and trailing dots must not survive.
		for _, seg := range strings.Split(clean, "/") {
			if strings.HasSuffix(seg, ".") {
				t.Fatalf("Rel(%q) returned trailing dot in %q", p, seg)
			}
			name := seg
			if dot := strings.IndexByte(name, '.'); dot >= 0 {
				name = name[:dot]
			}
			name = strings.TrimRight(name, " ")
			switch strings.ToUpper(name) {
			case "CON", "CONIN$", "CONOUT$", "CLOCK$", "PRN", "AUX", "NUL",
				"COM0", "COM1", "COM2", "COM3", "COM4",
				"COM5", "COM6", "COM7", "COM8", "COM9",
				"LPT0", "LPT1", "LPT2", "LPT3", "LPT4",
				"LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
				t.Fatalf("Rel(%q) returned reserved name %q", p, seg)
			}
		}
	})
}

func BenchmarkRel_HappyPath(b *testing.B) {
	const p = "deeply/nested/path/to/file.txt"
	allocs := testing.AllocsPerRun(1000, func() {
		_, _ = xfs.Rel(p)
	})
	if allocs != 0 {
		b.Fatalf("Rel allocated %v times per call, want 0", allocs)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = xfs.Rel(p)
	}
}
