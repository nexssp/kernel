package xerr

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// isDev is evaluated once at startup.
// Set ENV=development (or dev) or DEBUG=1 to enable verbose output.
var isDev = func() bool {
	env := strings.ToLower(os.Getenv("ENV"))
	return env == "development" || env == "dev" || os.Getenv("DEBUG") == "1"
}()

// frameNoise holds function-name prefixes that must never appear in a
// user-facing stack trace. Package-level so isUserFrame stays zero-alloc.
var frameNoise = []string{
	"runtime.", "reflect.", "testing.",
	"net/http.", "golang.org/x/",
}

// ErrChain walks errors.Unwrap and returns the full cause chain as a slice.
func ErrChain(err error) []error {
	var chain []error
	for err != nil {
		chain = append(chain, err)
		err = errors.Unwrap(err)
	}
	return chain
}

// Sprint returns a formatted error string.
//
//   - Dev:  full cause chain + resolved stack frames (file:line)
//   - Prod: kind + top-level message only — nothing internal leaks
func Sprint(err error) string {
	if err == nil {
		return ""
	}
	if isDev {
		return sprintDev(err)
	}
	return sprintProd(err)
}

// Print writes Sprint(err) to stderr.
func Print(err error) {
	fmt.Fprint(os.Stderr, Sprint(err))
}

// ── dev ──────────────────────────────────────────────────────────────────────

func sprintDev(err error) string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString("┌─ ERROR ────────────────────────────────────────────────────\n")

	depth := 0
	current := err
	for current != nil {
		indent := "│  "

		var appErr *AppError
		if errors.As(current, &appErr) {
			if depth == 0 {
				fmt.Fprintf(&b, "%sKind    : %s\n", indent, appErr.Kind)
				fmt.Fprintf(&b, "%sMessage : %s\n", indent, appErr.Message)
			} else {
				fmt.Fprintf(&b, "│\n%sCaused  : [%s] %s\n", indent, appErr.Kind, appErr.Message)
			}

			if len(appErr.Stack) > 0 {
				b.WriteString("│\n│  Stack:\n")
				frames := runtime.CallersFrames(appErr.Stack)
				printed := 0
				for printed < 8 {
					f, more := frames.Next()
					if isUserFrame(f.Function) {
						fmt.Fprintf(&b, "│    %s:%d\n", trimPath(f.File), f.Line)
						fmt.Fprintf(&b, "│      → %s\n", trimFunc(f.Function))
						printed++
					}
					if !more {
						break
					}
				}
			}

			current = appErr.Cause
		} else {
			// Plain Go error — walk the full Unwrap chain for context
			label := "Error"
			if depth > 0 {
				label = "Caused"
			}
			fmt.Fprintf(&b, "%s%s  : %v\n", indent, label, current)

			inner := errors.Unwrap(current)
			for inner != nil {
				fmt.Fprintf(&b, "%s          %v\n", indent, inner)
				inner = errors.Unwrap(inner)
			}
			break
		}
		depth++
	}

	b.WriteString("└────────────────────────────────────────────────────────────\n\n")
	return b.String()
}

// ── prod ─────────────────────────────────────────────────────────────────────

func sprintProd(err error) string {
	// Fast path: direct *AppError, no errors.As reflection alloc.
	if appErr, ok := err.(*AppError); ok {
		return formatAppError(appErr)
	}
	// Wrapped: errors.As unwraps once, then format.
	var appErr *AppError
	if errors.As(err, &appErr) {
		return formatAppError(appErr)
	}
	return err.Error() + "\n"
}

func formatAppError(appErr *AppError) string {
	var b strings.Builder
	b.Grow(len(appErr.Kind) + len(appErr.Message) + 4)
	b.WriteByte('[')
	b.WriteString(string(appErr.Kind))
	b.WriteString("] ")
	b.WriteString(appErr.Message)
	b.WriteByte('\n')
	return b.String()
}

// ── helpers ──────────────────────────────────────────────────────────────────

// isUserFrame skips stdlib, runtime, and internal third-party dependencies.
func isUserFrame(fn string) bool {
	for _, s := range frameNoise {
		if strings.Contains(fn, s) {
			return false
		}
	}
	return strings.Contains(fn, "/") // must have a package path
}

// trimPath shortens absolute paths to the nearest module boundary.
func trimPath(path string) string {
	normalized := filepath.ToSlash(path)

	for _, sep := range []string{"/nexss/", "/nexss-ai/", "/nexss_advanced/"} {
		if i := strings.LastIndex(normalized, sep); i >= 0 {
			return "..." + normalized[i:]
		}
	}

	parts := strings.Split(normalized, "/")
	if len(parts) > 2 {
		return ".../" + strings.Join(parts[len(parts)-2:], "/")
	}
	return normalized
}

// trimFunc removes the module-path prefix, keeping only "pkg.FuncName".
func trimFunc(fn string) string {
	if i := strings.LastIndex(fn, "/"); i >= 0 {
		return fn[i+1:]
	}
	return fn
}
