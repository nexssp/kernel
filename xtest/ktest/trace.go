package ktest

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

type Trace struct {
	mu    sync.Mutex
	calls []Call
	seq   uint64
}

type Call struct {
	Seq      uint64
	Action   string
	Request  any
	Response any
	Err      error
	Duration time.Duration
	When     time.Time
}

func NewTrace() *Trace { return &Trace{} }

func (t *Trace) Hook() action.AnyHook {
	return action.AnyHook{
		Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
			return context.WithValue(ctx, traceStartKey{}, time.Now()), nil
		},
		OnSuccess: func(ctx context.Context, req, res any, meta *action.Meta) {
			t.record(ctx, meta, req, res, nil)
		},
		OnError: func(ctx context.Context, req any, err error, meta *action.Meta) {
			t.record(ctx, meta, req, nil, err)
		},
	}
}

type traceStartKey struct{}

func (t *Trace) record(ctx context.Context, meta *action.Meta, req, res any, err error) {
	if meta == nil {
		return
	}
	start, _ := ctx.Value(traceStartKey{}).(time.Time)

	t.mu.Lock()
	t.seq++
	t.calls = append(t.calls, Call{
		Seq:      t.seq,
		Action:   meta.Name,
		Request:  req,
		Response: res,
		Err:      err,
		Duration: time.Since(start),
		When:     time.Now(),
	})
	t.mu.Unlock()
}

func (t *Trace) Calls() []Call {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]Call(nil), t.calls...)
}

func (t *Trace) Reset() {
	t.mu.Lock()
	t.calls = nil
	t.seq = 0
	t.mu.Unlock()
}

func (t *Trace) Names() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.calls))
	for i, c := range t.calls {
		out[i] = c.Action
	}
	return out
}

func (t *Trace) Count(name string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, c := range t.calls {
		if c.Action == name {
			n++
		}
	}
	return n
}

func (t *Trace) RequireSequence(tb testing.TB, want ...string) {
	tb.Helper()
	got := t.Names()
	if slices.Equal(got, want) {
		return
	}
	tb.Fatalf("trace sequence mismatch\n  want: %v\n  got:  %v", want, got)
}

func (t *Trace) RequireSubsequence(tb testing.TB, want ...string) {
	tb.Helper()
	got := t.Names()
	if len(want) == 0 {
		return
	}
	for i := 0; i+len(want) <= len(got); i++ {
		if slices.Equal(got[i:i+len(want)], want) {
			return
		}
	}
	tb.Fatalf("trace does not contain subsequence %v\n  got: %v", want, got)
}

func (t *Trace) RequireCalled(tb testing.TB, name string) {
	tb.Helper()
	if t.Count(name) == 0 {
		tb.Fatalf("action %q was never called; trace: %v", name, t.Names())
	}
}

func (t *Trace) RequireNotCalled(tb testing.TB, name string) {
	tb.Helper()
	if n := t.Count(name); n > 0 {
		tb.Fatalf("action %q was called %d time(s); trace: %v", name, n, t.Names())
	}
}

func (t *Trace) RequireOrder(tb testing.TB, first, second string) {
	tb.Helper()
	calls := t.Calls()
	for _, c := range calls {
		if c.Action == first {
			for _, c2 := range calls {
				if c2.Action == second && c2.Seq > c.Seq {
					return
				}
			}
			tb.Fatalf("action %q never appears before %q", first, second)
			return
		}
	}
	tb.Fatalf("action %q was never called; trace: %v", first, t.Names())
}

func (t *Trace) RequireAllSucceeded(tb testing.TB) {
	tb.Helper()
	for _, c := range t.Calls() {
		if c.Err != nil {
			tb.Fatalf("action %q failed: %v", c.Action, c.Err)
		}
	}
}

func (t *Trace) RequireErrorsAt(tb testing.TB, want ...string) {
	tb.Helper()
	var got []string
	for _, c := range t.Calls() {
		if c.Err != nil {
			got = append(got, c.Action)
		}
	}
	if !slices.Equal(got, want) {
		tb.Fatalf("error trace mismatch\n  want: %v\n  got:  %v", want, got)
	}
}

func (t *Trace) Dump(tb testing.TB) {
	tb.Helper()
	var sb strings.Builder
	sb.WriteString("trace:\n")
	for _, c := range t.Calls() {
		status := "OK"
		if c.Err != nil {
			status = "ERR"
		}
		fmt.Fprintf(&sb, "  [%3d] %-30s %-4s %s\n", c.Seq, c.Action, status, c.Duration)
	}
	tb.Log(sb.String())
}
