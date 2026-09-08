package observe_test

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/nexssp/kernel/observe"
)

func TestSlogSink_LevelsAndFields(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	sink := observe.NewSlogSink(slog.New(slog.NewTextHandler(&buf, nil)))
	sink.Emit(context.Background(), observe.Event{
		Kind: observe.KindExecuted, Action: "user.find",
		ExecutionID: "exec-1", TraceID: "trace-1", SpanID: "span-1",
	})
	out := buf.String()
	for _, want := range []string{"level=INFO", "event=executed", "action=user.find", "execution_id=exec-1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in log: %s", want, out)
		}
	}

	buf.Reset()
	sink.Emit(context.Background(), observe.Event{Kind: observe.KindError, Action: "x"})
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("expected ERROR level, got %s", buf.String())
	}
}

func TestSlogSink_NilLoggerUsesDefault(t *testing.T) {
	t.Parallel()
	sink := observe.NewSlogSink(nil)
	if sink == nil {
		t.Fatal("expected non-nil sink")
	}
	sink.Emit(context.Background(), observe.Event{Kind: observe.KindExecuted})
}

func TestMemorySink_RingOrderAndIsolation(t *testing.T) {
	t.Parallel()

	s := observe.NewMemorySink(3)
	ctx := context.Background()
	for _, kind := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		s.Emit(ctx, observe.Event{Kind: kind})
	}

	got := s.Events()
	want := []string{"e", "f", "g"}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Kind != want[i] {
			t.Fatalf("event %d: got %q, want %q", i, got[i].Kind, want[i])
		}
	}

	got[0].Kind = "mutated"
	if s.Events()[0].Kind == "mutated" {
		t.Fatal("Events returned the internal slice")
	}
}

func TestMemorySink_DefaultCapacity(t *testing.T) {
	t.Parallel()

	s := observe.NewMemorySink(0)
	for i := 0; i < 101; i++ {
		s.Emit(context.Background(), observe.Event{Kind: strconv.Itoa(i)})
	}
	got := s.Events()
	if len(got) != 100 {
		t.Fatalf("got %d events, want default capacity 100", len(got))
	}
	if got[0].Kind != "1" || got[99].Kind != "100" {
		t.Fatalf("unexpected default-capacity range: first=%q last=%q", got[0].Kind, got[99].Kind)
	}
}

func TestMemorySink_ZeroValue(t *testing.T) {
	t.Parallel()

	var s observe.MemorySink
	s.Emit(context.Background(), observe.Event{Kind: "ignored"})
	if got := s.Events(); len(got) != 0 {
		t.Fatalf("zero-value sink returned %+v, want empty snapshot", got)
	}
}

func TestMemorySink_ConcurrentWriteAndRead(t *testing.T) {
	t.Parallel()

	s := observe.NewMemorySink(100)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Emit(ctx, observe.Event{Kind: "event"})
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = s.Events()
			}
		}()
	}
	wg.Wait()
	if got := len(s.Events()); got != 100 {
		t.Fatalf("got %d events, want 100", got)
	}
}

func TestMetricsSink_CountingAndIsolation(t *testing.T) {
	t.Parallel()

	s := observe.NewMetricsSink()
	ctx := context.Background()
	s.Emit(ctx, observe.Event{Action: "a", Kind: observe.KindExecuted})
	s.Emit(ctx, observe.Event{Action: "a", Kind: observe.KindExecuted})
	s.Emit(ctx, observe.Event{Action: "a", Kind: observe.KindError})

	snapshot := s.Snapshot()
	key := observe.MetricKey{Action: "a", Kind: observe.KindExecuted}
	if snapshot[key] != 2 {
		t.Fatalf("got %d executed events, want 2", snapshot[key])
	}
	snapshot[key] = 999
	if got := s.Snapshot()[key]; got != 2 {
		t.Fatalf("snapshot mutation changed sink to %d", got)
	}
}

func TestMetricsSink_ConcurrentEmits(t *testing.T) {
	t.Parallel()

	s := observe.NewMetricsSink()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.Emit(ctx, observe.Event{Action: "conc", Kind: observe.KindExecuted})
			}
		}()
	}
	wg.Wait()
	key := observe.MetricKey{Action: "conc", Kind: observe.KindExecuted}
	if got := s.Snapshot()[key]; got != 1000 {
		t.Fatalf("got %d events, want 1000", got)
	}
}
