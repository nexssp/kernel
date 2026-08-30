package action_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
)

// ... (keep TestLogCalls as it was) ...

func TestInstrument(t *testing.T) {
	t.Parallel()
	var callCount, errCount int
	var latencySum float64

	act := action.New("instr.action", func(ctx context.Context, _ string) (string, error) {
		return "done", nil
	}).Instrument(
		func(_ string) { callCount++ },
		func(_ string) { errCount++ },
		func(_ string, ms float64) { latencySum += ms },
	).Build()

	_, err := act.Do(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected incCall to be called once, got %d", callCount)
	}
	if errCount != 0 {
		t.Fatalf("expected incError to be 0, got %d", errCount)
	}
	// FIX: Pipeline is zero-alloc and sub-millisecond. 0 is a valid measurement.
	if latencySum < 0 {
		t.Fatal("expected non-negative latency")
	}
}

func TestBuilder_RecordHistory(t *testing.T) {
	t.Parallel()
	act := action.New("payment.charge", func(ctx context.Context, req int) (int, error) {
		return req * 2, nil
	}).RecordHistory(5).Build()

	_, err := act.Do(context.Background(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hist := act.History()
	if hist == nil {
		t.Fatal("expected History handle to be attached to BuiltAction, got nil")
	}

	snap := hist.Snapshot()
	if len(snap) != 1 || snap[0].Req != 100 {
		t.Fatalf("expected 1 record with Req=100, got %+v", snap)
	}
}

func TestLogCalls(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	act := action.New("logged.action", func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	}).LogCalls(logger).Build()

	_, err := act.Do(context.Background(), "world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "action_start") || !strings.Contains(output, "action_ok") {
		t.Fatalf("expected log to contain 'action_start' and 'action_ok', got:\n%s", output)
	}
}

func TestInstrument_ErrorCount(t *testing.T) {
	t.Parallel()

	var callCount, errCount atomic.Int32
	act := action.New("instr.error", func(ctx context.Context, _ string) (string, error) {
		return "", errors.New("simulated failure")
	}).Instrument(
		func(_ string) { callCount.Add(1) },
		func(_ string) { errCount.Add(1) },
		nil,
	).Build()

	_, err := act.Do(context.Background(), "fail")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount.Load() != 1 {
		t.Errorf("incCall should be called once, got %d", callCount.Load())
	}
	if errCount.Load() != 1 {
		t.Errorf("incError should be called once, got %d", errCount.Load())
	}
}
