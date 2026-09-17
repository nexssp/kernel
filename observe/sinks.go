package observe

import (
	"context"
	"log/slog"
	"maps"
	"sync"

	"github.com/nexssp/kernel/ringbuf"
)

// SlogSink writes structured lifecycle events to a standard-library slog.Logger.
type SlogSink struct {
	logger *slog.Logger
}

// NewSlogSink creates a sink using logger. If logger is nil, slog.Default() is used.
func NewSlogSink(logger *slog.Logger) *SlogSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogSink{logger: logger}
}

func (s *SlogSink) Emit(ctx context.Context, event Event) {
	if s == nil || s.logger == nil {
		return
	}
	level := slog.LevelInfo
	if event.Kind == KindError {
		level = slog.LevelError
	}
	s.logger.Log(ctx, level, "nexss action",
		"event", event.Kind,
		"action", event.Action,
		"duration", event.Duration,
		"execution_id", event.ExecutionID,
		"trace_id", event.TraceID,
		"span_id", event.SpanID,
		"attempt", event.Attempt,
		"request", event.Request,
		"response", event.Response,
		"error", event.Error,
		"recovered", event.Recovered,
	)
}

type MemorySink struct {
	events *ringbuf.Buffer[Event]
}

func NewMemorySink(capacity int) *MemorySink {
	if capacity <= 0 {
		capacity = 100 // Safe default
	}
	return &MemorySink{
		events: ringbuf.NewBuffer[Event](capacity),
	}
}

func (s *MemorySink) Emit(_ context.Context, event Event) {
	if s == nil || s.events == nil {
		return
	}
	s.events.Push(event)
}

func (s *MemorySink) Events() []Event {
	if s == nil || s.events == nil {
		return nil
	}
	return s.events.Snapshot()
}

// MetricKey uniquely identifies a lifecycle event counter.
type MetricKey struct {
	Action string
	Kind   string
}

// MetricsSink aggregates lifecycle event counts in memory.
type MetricsSink struct {
	mu       sync.RWMutex
	counters map[MetricKey]uint64
}

// NewMetricsSink creates an empty metrics sink.
func NewMetricsSink() *MetricsSink {
	return &MetricsSink{counters: make(map[MetricKey]uint64)}
}

func (s *MetricsSink) Emit(_ context.Context, event Event) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.counters == nil {
		s.counters = make(map[MetricKey]uint64)
	}
	s.counters[MetricKey{Action: event.Action, Kind: event.Kind}]++
	s.mu.Unlock()
}

// Snapshot returns an isolated thread-safe copy of all counters.
func (s *MetricsSink) Snapshot() map[MetricKey]uint64 {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[MetricKey]uint64, len(s.counters))
	maps.Copy(out, s.counters)
	return out
}

var (
	_ Sink = (*SlogSink)(nil)
	_ Sink = (*MemorySink)(nil)
	_ Sink = (*MetricsSink)(nil)
)
