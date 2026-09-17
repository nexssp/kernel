package xerr

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// ── ErrChain ────────────────────────────────────────────────────────────────

func TestErrChain_Nil(t *testing.T) {
	if got := ErrChain(nil); got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}

func TestErrChain_Unwrap(t *testing.T) {
	base := errors.New("base")
	mid := fmt.Errorf("mid: %w", base)
	top := fmt.Errorf("top: %w", mid)

	chain := ErrChain(top)
	if len(chain) != 3 {
		t.Fatalf("want 3, got %d", len(chain))
	}
	if !errors.Is(chain[0], top) || !errors.Is(chain[1], mid) || !errors.Is(chain[2], base) {
		t.Fatalf("wrong order: %v", chain)
	}
}

// ── sprintProd — brak wycieków Cause na prod ────────────────────────────────

func TestSprintProd_DoesNotLeakCause(t *testing.T) {
	secret := errors.New("pq: password authentication failed for user admin")
	err := &AppError{
		Kind:    "internal",
		Message: "internal error",
		Cause:   secret,
	}

	out := sprintProd(err)

	if !strings.Contains(out, "[internal]") {
		t.Fatalf("missing kind: %q", out)
	}
	if !strings.Contains(out, "internal error") {
		t.Fatalf("missing message: %q", out)
	}
	if strings.Contains(out, "password") || strings.Contains(out, "admin") {
		t.Fatalf("cause leaked to prod: %q", out)
	}
}

func TestSprintProd_FindsWrappedAppError(t *testing.T) {
	inner := &AppError{Kind: "not_found", Message: "user not found"}
	wrapped := fmt.Errorf("handler: %w", inner)

	out := sprintProd(wrapped)
	want := "[not_found] user not found\n"
	if out != want {
		t.Fatalf("got %q, want %q", out, want)
	}
}

func TestSprintProd_DirectAppError_AllocBudget(t *testing.T) {
	err := &AppError{Kind: "internal", Message: "internal error"}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = sprintProd(err)
	})
	if allocs > 1 {
		t.Fatalf("sprintProd(*AppError) allocs = %v, want <= 1 — fast path removed?", allocs)
	}
}

func TestSprintProd_WrappedAppError_StillFormatted(t *testing.T) {
	inner := &AppError{Kind: "not_found", Message: "user not found"}
	err := fmt.Errorf("handler: %w", inner)

	if got, want := sprintProd(err), "[not_found] user not found\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSprintProd_PlainError(t *testing.T) {
	if got := sprintProd(errors.New("boom")); got != "boom\n" {
		t.Fatalf("got %q", got)
	}
}

// ── trimPath ────────────────────────────────────────────────────────────────

func TestTrimPath_Unix(t *testing.T) {
	in := "/home/dev/proj/nexss/kernel/xerr/format.go"
	out := trimPath(in)
	if !strings.Contains(out, "nexss") {
		t.Fatalf("module root lost: %q", out)
	}
	if strings.HasPrefix(out, "/home") {
		t.Fatalf("absolute prefix not trimmed: %q", out)
	}
}

func TestTrimPath_Fallback(t *testing.T) {
	in := "/opt/build/some/deep/tree/pkg/file.go"
	out := trimPath(in)
	if !strings.HasPrefix(out, ".../") {
		t.Fatalf("fallback not applied: %q", out)
	}
}

// TestTrimPath_Windows — odpalać tylko na GOOS=windows w CI.
// filepath.ToSlash na Linuxie nie zamienia "\" na "/", więc test
// musiałby być build-tagowany. Zostawiam jako dokumentację.
//
//	func TestTrimPath_Windows(t *testing.T) {
//	    in := `C:\Users\dev\proj\nexss\kernel\xerr\format.go`
//	    out := trimPath(in)
//	    if !strings.Contains(out, "nexss") {
//	        t.Fatalf("got %q", out)
//	    }
//	}

// ── isUserFrame ─────────────────────────────────────────────────────────────

func TestIsUserFrame(t *testing.T) {
	cases := []struct {
		fn   string
		want bool
	}{
		{"github.com/nexss/nexssp/kernel/xerr.sprintDev", true},
		{"main.main", false},
		{"errors.New", false},
		{"fmt.Errorf", false},
		{"runtime.gopanic", false},
		{"net/http.(*conn).serve", false},
		{"golang.org/x/sync/singleflight.(*Group).Do", false},
	}
	for _, c := range cases {
		if got := isUserFrame(c.fn); got != c.want {
			t.Errorf("isUserFrame(%q) = %v, want %v", c.fn, got, c.want)
		}
	}
}

// ── trimFunc ────────────────────────────────────────────────────────────────

func TestTrimFunc(t *testing.T) {
	in := "github.com/nexss/nexssp/kernel/xerr.(*AppError).Error"
	want := "xerr.(*AppError).Error"
	if got := trimFunc(in); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTrimFunc_NoSlash(t *testing.T) {
	if got := trimFunc("main.main"); got != "main.main" {
		t.Fatalf("got %q", got)
	}
}

// ── alloc budgets — łapią regresję na hot path ──────────────────────────────

// sprintProd ma alokować co najwyżej raz: zwracany string.
// Więcej = ktoś wrócił do fmt.Sprintf albo dodał map/slice.
func TestSprintProd_AllocsBudget(t *testing.T) {
	err := &AppError{Kind: "internal", Message: "internal error"}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = sprintProd(err)
	})
	if allocs > 2 {
		t.Fatalf("sprintProd allocs = %v, want ≤ 1", allocs)
	}
}

// isUserFrame musi być zero-alloc (frameNoise jako package var).
func TestIsUserFrame_ZeroAlloc(t *testing.T) {
	fn := "github.com/nexss/nexssp/kernel/xerr.sprintDev"

	allocs := testing.AllocsPerRun(1000, func() {
		_ = isUserFrame(fn)
	})
	if allocs != 0 {
		t.Fatalf("isUserFrame allocs = %v, want 0", allocs)
	}
}

// ── benchmarks ──────────────────────────────────────────────────────────────

func BenchmarkSprintProd_AppError(b *testing.B) {
	err := &AppError{Kind: "internal", Message: "internal error"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = sprintProd(err)
	}
}

func BenchmarkSprintProd_Plain(b *testing.B) {
	err := errors.New("boom")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = sprintProd(err)
	}
}

func BenchmarkIsUserFrame(b *testing.B) {
	fn := "github.com/nexss/nexssp/kernel/xerr.sprintDev"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = isUserFrame(fn)
	}
}

func BenchmarkTrimPath(b *testing.B) {
	p := "/home/dev/proj/nexss/kernel/xerr/format.go"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = trimPath(p)
	}
}

func BenchmarkSprintProd_DirectAppError(b *testing.B) {
	err := &AppError{Kind: "internal", Message: "internal error"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = sprintProd(err)
	}
}
