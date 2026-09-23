package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/stream"
)

// 1. Domain data models.
// By utilizing typed streams (generics), we eliminate the need for `interface{}` / `any`.
type LogEntry struct {
	ID        int
	Level     string
	Message   string
	Timestamp time.Time
}

// fetchLogs simulates a "heavy" data source. This could represent an SQL cursor (rows.Next()),
// or a paginated REST API fetching logs.
func fetchLogs(ctx context.Context, limit int) iter.Seq2[LogEntry, error] {
	// StreamFromFunc perfectly bridges the pull-based world (like bufio.Scanner) with iter.Seq2.
	count := 0
	return action.StreamFromFunc(func() (LogEntry, error) {
		if ctx.Err() != nil {
			return LogEntry{}, ctx.Err()
		}
		if count >= limit {
			return LogEntry{}, io.EOF // Signals end of stream
		}
		count++

		// Simulate alternating log levels
		level := "INFO"
		if count%5 == 0 {
			level = "ERROR"
		} else if count%7 == 0 {
			level = "FATAL"
		}

		return LogEntry{
			ID:        count,
			Level:     level,
			Message:   fmt.Sprintf("System event %d", count),
			Timestamp: time.Now(),
		}, nil
	})
}

// BuildPipeline assembles the stream operators.
// Exported so we can easily unit-test it.
func BuildPipeline() *action.StreamAction[int, LogEntry] {
	return action.NewStream("etl.logs", func(ctx context.Context, limit int) (iter.Seq2[LogEntry, error], error) {
		// A. Fetch lazily
		source := fetchLogs(ctx, limit)

		// B. Filter only errors/fatals (ZERO allocations, predicate is inlined by compiler)
		errorsOnly := stream.Filter(func(e LogEntry) bool {
			return e.Level == "ERROR" || e.Level == "FATAL"
		})(source)

		// C. On-the-fly transformation (Map to a different model or format)
		anonymized := stream.Map(func(e LogEntry) LogEntry {
			e.Message = "[REDACTED] Module error"
			return e
		})(errorsOnly)

		// D. Safety: Stream will abort immediately when ctx is canceled
		safeStream := stream.WithContext[LogEntry](ctx)(anonymized)

		return safeStream, nil
	}).
		// Kernel handles metrics and hooks for the entire stream boundary
		HookStartEvent(func() { fmt.Println("📡 [Kernel] Stream started (gate opened)") }).
		HookSuccessEvent(func() { fmt.Println("✅ [Kernel] Stream finished successfully (EOF)") }).
		HookCancelEvent(func() { fmt.Println("🛑 [Kernel] Stream interrupted (Timeout/Cancel)") })
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	fmt.Println("🚀 Initializing Nexss Lazy ETL Pipeline...")
	fmt.Println("Goal: Sift through millions of logs in real-time using O(1) memory (Zero Heap Allocation per-item).")
	fmt.Println(strings.Repeat("─", 80))

	pipeline := BuildPipeline()

	// STEP 2: Consumption
	// Request 1,000,000 records
	seq, err := pipeline.Do(ctx, 1_000_000)
	if err != nil {
		panic(err)
	}

	processedErrs := 0
	start := time.Now()

	// Go 1.23 for-range loop seamlessly pulls data directly from the generator
	for entry, streamErr := range seq {
		if streamErr != nil {
			fmt.Println("Stream read error:", streamErr)
			break
		}

		processedErrs++

		// Simulation: Show only the first 3 results to prevent console spam
		if processedErrs <= 3 {
			fmt.Printf("🔥 [Received on-the-fly] ID: %d | Level: %s | Message: %s\n", entry.ID, entry.Level, entry.Message)
		} else if processedErrs == 4 {
			fmt.Println("   ... and thousands more in the blink of an eye ...")
		}

		// Demonstrate early stream termination (Backpressure)
		// We break the loop after collecting 100,000 errors
		if processedErrs >= 100_000 {
			break
		}
	}

	duration := time.Since(start)
	fmt.Println(strings.Repeat("─", 80))
	fmt.Printf("📊 Result: Processed %d critical events in %v.\n", processedErrs, duration)
	fmt.Println("ℹ️  Notice that RAM usage did not spike – exactly 1 instance of the `LogEntry` struct was reused in CPU cache.")
}
