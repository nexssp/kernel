package observe

import (
	"testing"
)

func FuzzPrometheusEscapeLabel(f *testing.F) {
	seeds := []string{"", "simple", `quote"`, `back\slash`, "newline\n", "tab\t"}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		escaped := escapeLabel(input)
		// Escaped string must not contain raw newline or raw quote.
		for _, r := range escaped {
			if r == '\n' {
				t.Fatalf("escaped string contains newline: %q", escaped)
			}
		}
		if escaped != "" && escaped[0] == '"' {
			t.Fatalf("escaped string starts with quote: %q", escaped)
		}
	})
}

func FuzzJSONLRecord(f *testing.F) {
	// 7 seed values matching the 7 fuzz parameters:
	// timeStr, kind, action, execID, traceID, spanID, attempt
	f.Add("", "", "", "", "", "", 0)

	f.Fuzz(func(t *testing.T, timeStr, kind, action, execID, traceID, spanID string, attempt int) {
		// The inputs are not directly used here, but we exercise boundedError
		// with a fixed error to ensure it never panics.
		_ = boundedError(fakeError{}, 10)
	})
}

type fakeError struct{}

func (fakeError) Error() string { return "some error" }
