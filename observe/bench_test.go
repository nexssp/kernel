package observe_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/observe"
)

type noopSink struct{}

func (*noopSink) Emit(context.Context, observe.Event) {}

func TestHook_NilSink_ZeroAlloc(t *testing.T) {
	h := observe.Hook(nil)
	ctx := context.Background()
	meta := &action.Meta{Name: "bench.action"}
	allocs := testing.AllocsPerRun(1000, func() {
		if h.OnExecuted != nil {
			h.OnExecuted(ctx, "req", "res", nil, meta)
		}
	})
	if allocs != 0 {
		t.Fatalf("got %.2f allocations, want zero", allocs)
	}
}

func BenchmarkHook_NilSink(b *testing.B) {
	h := observe.Hook(nil)
	ctx := context.Background()
	meta := &action.Meta{Name: "bench.action"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if h.OnExecuted != nil {
			h.OnExecuted(ctx, "req", "res", nil, meta)
		}
	}
}

func BenchmarkHook_NoopSink(b *testing.B) {
	h := observe.Hook(&noopSink{})
	ctx := context.Background()
	meta := &action.Meta{Name: "bench.action"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.OnExecuted(ctx, "req", "res", nil, meta)
	}
}

func BenchmarkMetricsSink_Emit(b *testing.B) {
	sink := observe.NewMetricsSink()
	event := observe.Event{Action: "orders.create", Kind: observe.KindExecuted}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.Emit(context.Background(), event)
	}
}

func BenchmarkMemorySink_Emit(b *testing.B) {
	sink := observe.NewMemorySink(100)
	event := observe.Event{Action: "orders.create", Kind: observe.KindExecuted}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.Emit(context.Background(), event)
	}
}

func BenchmarkJSONLSink_Emit(b *testing.B) {
	sink := observe.NewJSONLSink(io.Discard, 4096)
	event := observe.Event{Action: "orders.create", Kind: observe.KindExecuted}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink.Emit(context.Background(), event)
	}
}

func BenchmarkPrometheusSink_Write(b *testing.B) {
	sink := observe.NewPrometheusSink()
	for i := 0; i < 10; i++ {
		sink.Emit(context.Background(), observe.Event{Action: "bench", Kind: observe.KindExecuted})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := sink.WritePrometheus(&buf); err != nil {
			b.Fatal(err)
		}
	}
}
