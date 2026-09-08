package observe_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/observe"
)

func TestPrometheusSink_DeterministicAndEscaping(t *testing.T) {
	t.Parallel()

	sink := observe.NewPrometheusSink()
	ctx := context.Background()
	sink.Emit(ctx, observe.Event{Action: `z.action\\"quoted`, Kind: observe.KindExecuted})
	sink.Emit(ctx, observe.Event{Action: "a.action", Kind: observe.KindError})
	sink.Emit(ctx, observe.Event{Action: `z.action\\"quoted`, Kind: observe.KindExecuted})

	var buf bytes.Buffer
	if err := sink.WritePrometheus(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `action="z.action\\\\\"quoted",kind="executed"} 2`) {
		t.Fatalf("escaping/count failed: %s", out)
	}
	if strings.Index(out, "a.action") > strings.Index(out, "z.action") {
		t.Fatalf("output is not sorted: %s", out)
	}
}

func TestPrometheusSink_NilWriterReturnsError(t *testing.T) {
	t.Parallel()
	if err := observe.NewPrometheusSink().WritePrometheus(nil); err == nil {
		t.Fatal("expected error for nil writer")
	}
	var sink *observe.PrometheusSink
	if err := sink.WritePrometheus(io.Discard); err == nil {
		t.Fatal("expected error for nil sink")
	}
}

func TestJSONLSink_FieldsAndBoundedError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	sink := observe.NewJSONLSink(&buf, 16)
	sink.Emit(context.Background(), observe.Event{
		Time: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Kind: observe.KindError, Action: "account.lookup",
		ExecutionID: "exec-9", TraceID: "trace-8", SpanID: "span-7", Attempt: 3,
		Request: "req-data", Error: errors.New("very long error message that exceeds max bytes"),
	})
	line := buf.String()
	for _, want := range []string{`"kind":"error"`, `"action":"account.lookup"`, `"execution_id":"exec-9"`, `"trace_id":"trace-8"`, `"span_id":"span-7"`, `"attempt":3`} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %q in %s", want, line)
		}
	}
	if strings.Contains(line, "very long error message") || !strings.HasSuffix(line, "\n") {
		t.Fatalf("error was not bounded or line lacks newline: %q", line)
	}
}

func TestJSONLSink_DoesNotSerializePayloadValues(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	sink := observe.NewJSONLSink(&buf, 64)
	sink.Emit(context.Background(), observe.Event{
		Kind: observe.KindExecuted, Action: "user.login",
		Request: struct{ Password string }{Password: "supersecret"},
	})
	out := buf.String()
	if strings.Contains(out, "supersecret") {
		t.Fatalf("payload leaked into JSONL: %s", out)
	}
	if !strings.Contains(out, `"request_type":"struct { Password string }"`) {
		t.Fatalf("request type missing: %s", out)
	}
}

func TestJSONLSink_NilWriterDoesNothing(t *testing.T) {
	t.Parallel()
	observe.NewJSONLSink(nil, 100).Emit(context.Background(), observe.Event{Kind: "x"})
}

func TestJSONLSink_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	sink := observe.NewJSONLSink(&buf, 100)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				sink.Emit(context.Background(), observe.Event{Kind: "event", Action: "concurrent"})
			}
		}()
	}
	wg.Wait()
	if lines := strings.Count(buf.String(), "\n"); lines != 200 {
		t.Fatalf("got %d lines, want 200", lines)
	}
}
