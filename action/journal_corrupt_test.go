package action_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

func TestDurable_InvalidJournalPayloadDoesNotRepeatStep(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte("{"), []byte("not-json")} {
		journal := newMemJournal()
		journal.records["run\x00step"] = payload
		var calls atomic.Int32
		step := action.New("durable.invalid", func(context.Context, int) (int, error) {
			calls.Add(1)
			return 42, nil
		}).Durable(journal, "step").Build()

		if _, err := step.Do(xctx.WithExecutionID(context.Background(), "run"), 1); err == nil {
			t.Fatalf("payload %q unexpectedly replayed by rerunning the step", payload)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("handler ran %d times for invalid journal payload %q", got, payload)
		}
	}
}
