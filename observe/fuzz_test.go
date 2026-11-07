package observe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func FuzzPrometheusEscapeLabel(f *testing.F) {
	seeds := []string{"", "simple", `quote"`, `back\slash`, "newline\n", "tab\t"}
	for _, value := range seeds {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, input string) {
		escaped := escapeLabel(input)
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
	f.Add("", "", "", "", "", 0)
	f.Fuzz(func(t *testing.T, kind, action, execID, traceID, spanID string, attempt int) {
		var output bytes.Buffer
		sink := NewJSONLSink(&output, 0)
		sink.Emit(context.Background(), Event{
			Kind: kind, Action: action,
			ExecutionID: execID, TraceID: traceID, SpanID: spanID,
			Attempt: attempt, Error: errors.New("fuzz"), Recovered: "fuzz",
		})
		line := output.Bytes()
		if strings.Count(string(line), "\n") != 1 {
			t.Fatalf("got != 1 newline: %q", line)
		}
		var record jsonRecord
		if err := json.Unmarshal(bytes.TrimSuffix(line, []byte{'\n'}), &record); err != nil {
			t.Fatalf("invalid JSONL record: %v", err)
		}
		if record.Error != "[redacted]" || record.Recovered != "[redacted]" {
			t.Fatalf("diagnostics were not redacted: %+v", record)
		}
	})
}
