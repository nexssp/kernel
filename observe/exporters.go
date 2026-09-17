package observe

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PrometheusSink struct {
	metrics *MetricsSink
}

func NewPrometheusSink() *PrometheusSink {
	return &PrometheusSink{metrics: NewMetricsSink()}
}

func (s *PrometheusSink) Emit(ctx context.Context, event Event) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.Emit(ctx, event)
}

func (s *PrometheusSink) Snapshot() map[MetricKey]uint64 {
	if s == nil || s.metrics == nil {
		return nil
	}
	return s.metrics.Snapshot()
}

func (s *PrometheusSink) WritePrometheus(w io.Writer) error {
	if s == nil || w == nil {
		return errors.New("observe: prometheus sink and writer are required")
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

type JSONLSink struct {
	mu       sync.Mutex
	writer   io.Writer
	maxBytes int
}

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
		Duration:     event.Duration.String(),
		Kind:         event.Kind,
		Action:       event.Action,
		RequestID:    event.RequestID,
		ExecutionID:  event.ExecutionID,
		TraceID:      event.TraceID,
		SpanID:       event.SpanID,
		TenantID:     event.TenantID,
		UserID:       event.UserID,
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
	if _, err := s.writer.Write(append(payload, '\n')); err != nil {
		slog.Warn("jsonl_sink_write_failed", "error", err)
	}
}

type jsonRecord struct {
	Time         time.Time `json:"time"`
	Duration     string    `json:"duration,omitempty"`
	Kind         string    `json:"kind"`
	Action       string    `json:"action"`
	RequestID    string    `json:"request_id,omitempty"`
	ExecutionID  string    `json:"execution_id,omitempty"`
	TraceID      string    `json:"trace_id,omitempty"`
	SpanID       string    `json:"span_id,omitempty"`
	TenantID     string    `json:"tenant_id,omitempty"`
	UserID       string    `json:"user_id,omitempty"`
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
	slices.SortFunc(keys, compareMetricKeys)
	return keys
}

func compareMetricKeys(a, b MetricKey) int {
	if c := cmp.Compare(a.Action, b.Action); c != 0 {
		return c
	}
	return cmp.Compare(a.Kind, b.Kind)
}

var (
	_ Sink = (*PrometheusSink)(nil)
	_ Sink = (*JSONLSink)(nil)
)
