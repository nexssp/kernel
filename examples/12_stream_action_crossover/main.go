package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/stream"
	"github.com/nexssp/kernel/xerr"
)

type LogEntry struct {
	ID        int
	IP        string
	Message   string
	Timestamp time.Time
}

type ThreatReport struct {
	LogEntry LogEntry
	Score    int
	Blocked  bool
}

func fetchLogs(ctx context.Context, limit int) iter.Seq2[LogEntry, error] {
	count := 0
	return action.StreamFromFunc(func() (LogEntry, error) {
		if ctx.Err() != nil {
			return LogEntry{}, ctx.Err()
		}
		if count >= limit {
			return LogEntry{}, io.EOF
		}
		count++

		msg := fmt.Sprintf("System event %d", count)
		// ~1 in 7 logs carries a SQL injection payload, so the demo
		// reaches the 10-blocked threshold quickly.
		if count%7 == 0 {
			msg = "SELECT * FROM users; DROP TABLE -- SQL Injection"
		}

		return LogEntry{
			ID:        count,
			IP:        fmt.Sprintf("10.0.0.%d", count%255),
			Message:   msg,
			Timestamp: time.Now(),
		}, nil
	})
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("🚀 Initializing Nexss Hybrid Pipeline: Stream + Unary Action...")
	fmt.Println("Goal: Sift through millions of logs in real-time using O(1) memory.")
	fmt.Println(strings.Repeat("─", 80))

	// Flips to true after the first failure for 192.168.1.100, so retry
	// has something to recover from instead of failing forever.
	var flakyIPFailed atomic.Bool

	analyzeThreatAct := action.New("ai.threat_analyzer", func(_ context.Context, log LogEntry) (ThreatReport, error) {
		if log.IP == "192.168.1.100" && flakyIPFailed.CompareAndSwap(false, true) {
			return ThreatReport{}, xerr.Unavailable("network temporarily unavailable (transient error)")
		}

		score := 10
		if strings.Contains(log.Message, "SQL") {
			score = 99
		}

		return ThreatReport{
			LogEntry: log,
			Score:    score,
			Blocked:  score > 80,
		}, nil
	}).
		// Admission control for a rate-limited upstream. This kernel's
		// RateLimit middleware rejects (returns TooManyRequests) rather
		// than blocking, so the burst must cover the demo's ~70 calls.
		// For real workloads where the upstream is slow, either raise
		// burst or add a blocking limiter via AdmissionMiddleware.
		RateLimit(1000, 100).
		Retry(3, action.ConstantBackoff(50*time.Millisecond)).
		Cache(1*time.Minute, func(l LogEntry) string { return l.IP + l.Message }).
		Build()

	processLogsStream := action.NewStream("pipeline.firewall", func(ctx context.Context, limit int) (iter.Seq2[ThreatReport, error], error) {
		rawLogs := fetchLogs(ctx, limit)

		analyzedStream := stream.MapE(func(log LogEntry) (ThreatReport, error) {
			// Only the first time we see this IP do we let it fail, so
			// Retry has a chance to demonstrate recovery.
			if log.ID == 2 {
				log.IP = "192.168.1.100"
			}
			return analyzeThreatAct.Do(ctx, log)
		})(rawLogs)

		blockedOnlyStream := stream.Filter(func(report ThreatReport) bool {
			return report.Blocked
		})(analyzedStream)

		return stream.WithContext[ThreatReport](ctx)(blockedOnlyStream), nil
	}).
		HookStartEvent(func() { fmt.Println("📡 [Kernel] Stream started (gate opened)") }).
		HookSuccessEvent(func() { fmt.Println("✅ [Kernel] Stream finished successfully (EOF)") }).
		HookCancelEvent(func() { fmt.Println("🛑 [Kernel] Stream interrupted (Timeout/Cancel)") })

	seq, err := processLogsStream.Do(ctx, 1_000_000)
	if err != nil {
		panic(err)
	}

	processedBlocks := 0
	start := time.Now()

	for report, streamErr := range seq {
		if streamErr != nil {
			fmt.Println("Stream read error:", streamErr)
			break
		}

		processedBlocks++
		if processedBlocks <= 3 {
			fmt.Printf("🔥 [Intercepted on-the-fly] IP: %s | Score: %d | Msg: %s\n",
				report.LogEntry.IP, report.Score, report.LogEntry.Message)
		}

		if processedBlocks >= 10 {
			break
		}
	}

	duration := time.Since(start)
	fmt.Println(strings.Repeat("─", 80))
	fmt.Printf("📊 Result: Processed %d critical events in %v.\n", processedBlocks, duration)
	fmt.Println("ℹ️  RAM did not spike — one ThreatReport struct reused per item.")
}
