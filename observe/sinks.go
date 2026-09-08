package observe

import (
	"context"
	"log/slog"
	"sync"
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

// MemorySink stores recent events up to Capacity in a thread-safe ring buffer.
// Intended for unit tests, local debugging, and CLI diagnostic inspection.
type MemorySink struct {
	mu       sync.Mutex
	events   []Event
	capacity int
	head     int // Tracks the next write position
	size     int // Tracks current total items stored
}

func NewMemorySink(capacity int) *MemorySink {
	if capacity <= 0 {
		capacity = 100 // Safe default
	}
	return &MemorySink{
		events:   make([]Event, capacity), // Pre-allocate fixed size
		capacity: capacity,
	}
}

func (s *MemorySink) Emit(_ context.Context, event Event) {
	if s == nil || s.capacity == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Overwrite the slot at head without shifting any memory
	s.events[s.head] = event

	// Advance head pointer circularly
	s.head = (s.head + 1) % s.capacity

	if s.size < s.capacity {
		s.size++
	}
}

// Events returns a snapshot copy in oldest-to-newest order.
func (s *MemorySink) Events() []Event {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// If no events have been recorded, return an empty slice
	if s.size == 0 {
		return []Event{}
	}

	result := make([]Event, s.size)

	// Case 1: Buffer has not wrapped around yet (it's a normal sequential slice)
	if s.size < s.capacity {
		copy(result, s.events[:s.size])
		return result
	}

	// Case 2: Buffer has wrapped around. The oldest item is right at the 'head' cursor.
	// Copy from 'head' to the end of the internal array
	copied := copy(result, s.events[s.head:])
	// Copy the remainder from the beginning of the array up to 'head'
	copy(result[copied:], s.events[:s.head])

	return result
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
	for key, count := range s.counters {
		out[key] = count
	}
	return out
}

var (
	_ Sink = (*SlogSink)(nil)
	_ Sink = (*MemorySink)(nil)
	_ Sink = (*MetricsSink)(nil)
)
