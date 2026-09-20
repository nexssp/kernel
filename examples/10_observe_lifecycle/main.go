// Example: complete lifecycle observation with logs, bounded memory, metrics,
// Prometheus, JSONL, retries, cache events, cancellation, panic recovery,
// request coalescing, request deduplication, and correlation IDs.
//
// Run from the repository root with:
//
//	go run ./examples/10_observe_lifecycle
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/observe"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

type fanoutSink struct{ sinks []observe.Sink }

func (s *fanoutSink) Emit(ctx context.Context, event observe.Event) {
	for _, sink := range s.sinks {
		if sink != nil {
			sink.Emit(ctx, event)
		}
	}
}

func main() {
	ctx := xctx.WithTraceContext(
		xctx.WithExecutionID(context.Background(), "exec-example-0001"),
		"trace-4bf92f3577b34da6a3ce929d0e0e4736",
		"span-00f067aa0ba902b7",
	)

	jsonlFile := mustCreate("observe.jsonl")
	defer closeFile(jsonlFile, "JSONL")
	slogFile := mustCreate("observe.log")
	defer closeFile(slogFile, "slog")

	memorySink := observe.NewMemorySink(100)
	metricsSink := observe.NewMetricsSink()
	prometheusSink := observe.NewPrometheusSink()
	observationSink := &fanoutSink{sinks: []observe.Sink{
		observe.NewSlogSink(slog.New(slog.NewJSONHandler(slogFile, nil))),
		memorySink,
		metricsSink,
		prometheusSink,
		observe.NewJSONLSink(jsonlFile, 4096),
	}}
	hook := observe.Hook(observationSink)

	var attempts atomic.Int32
	payment := action.New("payment.process", func(_ context.Context, amount float64) (string, error) {
		if amount <= 0 {
			return "", xerr.Validation("amount must be positive")
		}
		if attempts.Add(1) == 1 {
			return "", xerr.Unavailable("temporary payment gateway timeout")
		}
		return "payment settled", nil
	}).Retry(1, action.ConstantBackoff(10*time.Millisecond)).AnyHook(hook).Build()
	defer payment.Close()
	runBasicActionScenarios(ctx, hook)
	runConcurrencyScenarios(ctx, hook)

	fmt.Println("\n=== Recent observation events ===")
	events := memorySink.Events()
	for i := range events {
		event := &events[i]
		fmt.Printf("%-14s action=%-20s execution_id=%s trace_id=%s span_id=%s attempt=%d recovered=%v error=%v\n",
			event.Kind, event.Action, event.ExecutionID, event.TraceID, event.SpanID,
			event.Attempt, event.Recovered, event.Error)
	}

	fmt.Println("\n=== Aggregated metrics ===")
	metrics := metricsSink.Snapshot()
	keys := make([]observe.MetricKey, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Action == keys[j].Action {
			return keys[i].Kind < keys[j].Kind
		}
		return keys[i].Action < keys[j].Action
	})
	for _, key := range keys {
		fmt.Printf("%-20s %-16s %d\n", key.Action, key.Kind, metrics[key])
	}

	fmt.Println("\n=== Prometheus exposition ===")
	if err := prometheusSink.WritePrometheus(os.Stdout); err != nil {
		fatal("write Prometheus exposition", err)
	}
	fmt.Println("\nObservation logs written to observe.log and observe.jsonl")
}

func mustCreate(name string) *os.File {
	file, err := os.Create(name)
	if err != nil {
		fatal("create "+name, err)
	}
	return file
}

func closeFile(file *os.File, label string) {
	if err := file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close %s output: %v\n", label, err)
	}
}

func fatal(operation string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", operation, err)
	os.Exit(1)
}

func runBasicActionScenarios(ctx context.Context, hook action.AnyHook) {
	var attempts atomic.Int32
	payment := action.New("payment.process", func(_ context.Context, amount float64) (string, error) {
		if amount <= 0 {
			return "", xerr.Validation("amount must be positive")
		}
		if attempts.Add(1) == 1 {
			return "", xerr.Unavailable("temporary payment gateway timeout")
		}
		return "payment settled", nil
	}).Retry(1, action.ConstantBackoff(10*time.Millisecond)).AnyHook(hook).Build()
	defer payment.Close()
	if _, err := payment.Do(ctx, 99.95); err != nil {
		slog.Debug("payment result", "error", err)
	}

	failed := action.New("payment.validate", func(context.Context, string) (string, error) {
		return "", xerr.Validation("card number is invalid")
	}).AnyHook(hook).Build()
	if _, err := failed.Do(ctx, "bad-card"); err != nil {
		slog.Debug("failed validation result", "error", err)
	}

	findUser := action.New("user.find", func(_ context.Context, id string) (string, error) {
		return "User_" + id, nil
	}).Cache(time.Minute, func(id string) string { return id }).AnyHook(hook).Build()
	defer findUser.Close()
	for _, id := range []string{"alice", "alice", "bob"} {
		if _, err := findUser.Do(ctx, id); err != nil {
			slog.Debug("findUser result", "error", err)
		}
	}

	cancelAction := action.New("request.cancel", func(ctx context.Context, _ string) (string, error) {
		return "", ctx.Err()
	}).AnyHook(hook).Build()
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := cancelAction.Do(cancelCtx, "canceled-request"); err != nil {
		slog.Debug("cancelAction result", "error", err)
	}

	panicAction := action.New("worker.panic", func(context.Context, string) (string, error) {
		panic("simulated worker panic")
	}).AnyHook(hook).Build()
	if _, err := panicAction.Do(ctx, "panic-request"); err != nil {
		slog.Debug("panicAction result", "error", err)
	}
}

func runConcurrencyScenarios(ctx context.Context, hook action.AnyHook) {
	dedupEntered := make(chan struct{})
	dedupRelease := make(chan struct{})
	dedupSecondSeen := make(chan struct{})
	var dedupOnce sync.Once
	var dedupRequests atomic.Int32
	dedup := action.New("product.price", func(context.Context, string) (string, error) {
		dedupOnce.Do(func() { close(dedupEntered) })
		<-dedupRelease
		return "19.99", nil
	}).Dedup(func(id string) string {
		if dedupRequests.Add(1) == 2 {
			close(dedupSecondSeen)
		}
		return id
	}).AnyHook(hook).Build()
	var dedupWG sync.WaitGroup
	dedupWG.Go(func() {
		if _, err := dedup.Do(ctx, "sku-123"); err != nil {
			slog.Debug("dedup error", "error", err)
		}
	})
	<-dedupEntered
	dedupWG.Go(func() {
		if _, err := dedup.Do(ctx, "sku-123"); err != nil {
			slog.Debug("dedup error", "error", err)
		}
	})
	<-dedupSecondSeen
	close(dedupRelease)
	dedupWG.Wait()

	coalesceEntered := make(chan struct{})
	coalesceRelease := make(chan struct{})
	coalesceSecondSeen := make(chan struct{})
	var coalesceOnce sync.Once
	var coalesceRequests atomic.Int32
	coalescer := action.NewCoalescer()
	coalesced := action.New("inventory.check", func(context.Context, string) (string, error) {
		coalesceOnce.Do(func() { close(coalesceEntered) })
		<-coalesceRelease
		return "in-stock", nil
	}).Coalesce(coalescer, func(sku string) string {
		if coalesceRequests.Add(1) == 2 {
			close(coalesceSecondSeen)
		}
		return sku
	}).AnyHook(hook).Build()
	var coalesceWG sync.WaitGroup
	coalesceWG.Go(func() {
		if _, err := coalesced.Do(ctx, "sku-123"); err != nil {
			slog.Debug("coalesced error", "error", err)
		}
	})
	<-coalesceEntered
	coalesceWG.Go(func() {
		if _, err := coalesced.Do(ctx, "sku-123"); err != nil {
			slog.Debug("coalesced error", "error", err)
		}
	})
	<-coalesceSecondSeen
	close(coalesceRelease)
	coalesceWG.Wait()
}
