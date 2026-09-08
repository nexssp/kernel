package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PrometheusSink aggregates lifecycle counters and formats text exposition output.
type PrometheusSink struct {
	metrics *MetricsSink
}

// NewPrometheusSink creates a Prometheus text exporter sink.
func NewPrometheusSink() *PrometheusSink {
	return &PrometheusSink{metrics: NewMetricsSink()}
}

func (s *PrometheusSink) Emit(ctx context.Context, event Event) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.Emit(ctx, event)
}

// Snapshot returns a copy of current metric counters.
func (s *PrometheusSink) Snapshot() map[MetricKey]uint64 {
	if s == nil || s.metrics == nil {
		return nil
	}
	return s.metrics.Snapshot()
}

// WritePrometheus writes deterministic Prometheus text exposition lines to w.
func (s *PrometheusSink) WritePrometheus(w io.Writer) error {
	if s == nil || w == nil {
		return fmt.Errorf("observe: prometheus sink and writer are required")
	}
	snapshot := s.Snapshot()
	keys := sortedMetricKeys(snapshot)
	if _, err := io.WriteString(w, "# TYPE nexss_action_events_total counter\n"); err != nil {
		return err
	}
	for _, key := range keys {
		line := "nexss_action_events_total{" +
			"action=\"" + escapeLabel(key.Action) + "\"," +
			"kind=\"" + escapeLabel(key.Kind) + "\"} " +
			strconv.FormatUint(snapshot[key], 10) + "\n"
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
	}
	return nil
}

// JSONLSink emits bounded structured lifecycle records to w.
type JSONLSink struct {
	mu       sync.Mutex
	writer   io.Writer
	maxBytes int
}

// NewJSONLSink creates a synchronized JSONL sink. Non-positive maxBytes defaults to 4096.
func NewJSONLSink(writer io.Writer, maxBytes int) *JSONLSink {
	if maxBytes <= 0 {
		maxBytes = 4096
	}
	return &JSONLSink{writer: writer, maxBytes: maxBytes}
}

func (s *JSONLSink) Emit(_ context.Context, event Event) {
	if s == nil || s.writer == nil {
		return
	}
	record := jsonRecord{
		Time:         event.Time.UTC(),
		Kind:         event.Kind,
		Action:       event.Action,
		ExecutionID:  event.ExecutionID,
		TraceID:      event.TraceID,
		SpanID:       event.SpanID,
		Attempt:      event.Attempt,
		RequestType:  safeType(event.Request),
		ResponseType: safeType(event.Response),
		Error:        boundedError(event.Error, s.maxBytes),
		Recovered:    boundedValue(event.Recovered, s.maxBytes),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.writer.Write(append(payload, '\n'))
}

type jsonRecord struct {
	Time         time.Time `json:"time"`
	Kind         string    `json:"kind"`
	Action       string    `json:"action"`
	ExecutionID  string    `json:"execution_id,omitempty"`
	TraceID      string    `json:"trace_id,omitempty"`
	SpanID       string    `json:"span_id,omitempty"`
	Attempt      int       `json:"attempt,omitempty"`
	RequestType  string    `json:"request_type,omitempty"`
	ResponseType string    `json:"response_type,omitempty"`
	Error        string    `json:"error,omitempty"`
	Recovered    string    `json:"recovered,omitempty"`
}

func safeType(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%T", value)
}

func boundedError(err error, maxBytes int) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > maxBytes {
		return text[:maxBytes]
	}
	return text
}

func boundedValue(value any, maxBytes int) string {
	if value == nil {
		return ""
	}
	text := fmt.Sprint(value)
	if len(text) > maxBytes {
		return text[:maxBytes]
	}
	return text
}

func escapeLabel(value string) string {
	return strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n").Replace(value)
}

func sortedMetricKeys(values map[MetricKey]uint64) []MetricKey {
	keys := make([]MetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && metricKeyLess(keys[j], keys[j-1]); j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func metricKeyLess(left, right MetricKey) bool {
	if left.Action == right.Action {
		return left.Kind < right.Kind
	}
	return left.Action < right.Action
}

var (
	_ Sink = (*PrometheusSink)(nil)
	_ Sink = (*JSONLSink)(nil)
)
