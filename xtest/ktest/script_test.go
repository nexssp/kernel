package ktest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/xtest/ktest"
)

var errScript = errors.New("terminal error")

func TestScript_SequenceAndMustHelpers(t *testing.T) {
	t.Parallel()

	fn := ktest.Script[int, string](
		ktest.Success("step-1"),
		ktest.Failure[string](errScript),
		ktest.Success("step-final"),
	)

	if got := ktest.MustExecute(t, fn, 1); got != "step-1" {
		t.Fatalf("step 1 = %q", got)
	}

	ktest.MustError(t, fn, 2, errScript)

	if got := ktest.MustExecute(t, fn, 3); got != "step-final" {
		t.Fatalf("step 3 = %q", got)
	}

	// Repeats the last value.
	if got := ktest.MustExecute(t, fn, 4); got != "step-final" {
		t.Fatalf("step 4 = %q", got)
	}
}

func TestRecorder_AllHookEvents(t *testing.T) {
	t.Parallel()

	rec := new(ktest.Recorder[string, string])
	ctx := context.Background()

	rec.OnCacheHit(ctx, "req", "cached")
	rec.OnCacheMiss(ctx, "req")
	rec.OnCoalesced(ctx, "req")
	rec.OnDeduplicated(ctx, "req")
	rec.OnRetry(ctx, "req", 1, errors.New("err"))

	if rec.CacheHits != 1 || rec.CacheMisses != 1 || rec.Coalesced != 1 || rec.Deduplicated != 1 {
		t.Fatalf("recorder counter mismatch: %+v", rec)
	}
	if len(rec.Retries) != 1 || rec.Retries[0].Attempt != 1 {
		t.Fatalf("recorder retry tracking failed: %+v", rec.Retries)
	}
}
