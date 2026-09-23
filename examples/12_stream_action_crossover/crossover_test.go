package main

import (
	"context"
	"iter"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/stream"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestLogPipeline_FilteringAndErrorPropagation(t *testing.T) {
	ctx := context.Background()

	mockSource := ktest.StreamOf("mock.source",
		LogEntry{IP: "1.1.1.1", Message: "Safe"},
		LogEntry{IP: "2.2.2.2", Message: "SQL Injection"},
	)

	mockAnalyzer := ktest.Returns[LogEntry, ThreatReport]("mock.ai", ThreatReport{
		Score:   99,
		Blocked: true,
	}).Build()

	pipeline := action.NewStream("test.pipeline", func(ctx context.Context, _ struct{}) (iter.Seq2[ThreatReport, error], error) {
		seq, err := mockSource.Do(ctx, struct{}{})
		if err != nil {
			return nil, err
		}

		enriched := stream.MapE(func(log LogEntry) (ThreatReport, error) {
			return mockAnalyzer.Do(ctx, log)
		})(seq)

		return enriched, nil
	})

	outSeq, err := pipeline.Do(ctx, struct{}{})
	ktest.RequireNoError(t, err)

	var results []ThreatReport
	for item, err := range outSeq {
		ktest.RequireNoError(t, err)
		results = append(results, item)
	}

	ktest.RequireEqual(t, len(results), 2)
	ktest.RequireEqual(t, results[0].Score, 99)
}

func TestLogPipeline_PropagatesFatalErrors(t *testing.T) {
	ctx := context.Background()

	brokenSource := ktest.StreamFail[LogEntry]("broken.source", xerr.Database("Connection refused"))

	pipeline := action.NewStream("test.broken.pipeline", func(ctx context.Context, _ struct{}) (iter.Seq2[LogEntry, error], error) {
		return brokenSource.Do(ctx, struct{}{})
	})

	_, err := pipeline.Do(ctx, struct{}{})

	ktest.RequireErrorKind(t, err, xerr.KindDatabase)
	ktest.RequireErrorContains(t, err, "Connection refused")
}
