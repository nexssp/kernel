package action_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest"
)

func TestNilCoalescerFailsClosed(t *testing.T) {
	var coalescer *action.Coalescer
	called := 0
	got, shared, err := coalescer.Do(context.Background(), "key", func(context.Context) (any, error) {
		called++
		return "direct", nil
	})
	if err == nil || err.Error() != "action: coalescer is required" || got != nil || shared || called != 0 {
		t.Fatalf("Do = (%v, shared=%v, err=%v), calls=%d; want a nil-Coalescer error without running work", got, shared, err, called)
	}
	if got := coalescer.Waiters("key"); got != 0 {
		t.Fatalf("Waiters on nil Coalescer = %d, want 0", got)
	}

	keyCalls, handlerCalls := 0, 0
	act := action.New("nil.coalescer", func(_ context.Context, req string) (string, error) {
		handlerCalls++
		return req, nil
	}).Coalesce(coalescer, func(req string) string {
		keyCalls++
		return req
	}).Build()
	result, err := act.Do(context.Background(), "request")
	if err == nil || !strings.Contains(err.Error(), "action: coalescer is required") || result != "" || keyCalls != 0 || handlerCalls != 0 {
		t.Fatalf("Do = (%q, %v), key calls=%d handler calls=%d; want fail-closed validation", result, err, keyCalls, handlerCalls)
	}
}

func TestCoalesceSeparatesActionAndRequestKeyBoundaries(t *testing.T) {
	coalescer := action.NewCoalescer()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})

	first := action.New("foo", func(context.Context, string) (string, error) {
		close(firstStarted)
		<-releaseFirst
		return "first", nil
	}).Coalesce(coalescer, func(string) string { return "bar:baz" }).Build()
	second := action.New("foo:bar", func(context.Context, string) (string, error) {
		close(secondStarted)
		return "second", nil
	}).Coalesce(coalescer, func(string) string { return "baz" }).Build()

	type result struct {
		value string
		err   error
	}
	firstResult := make(chan result, 1)
	secondResult := make(chan result, 1)
	go func() {
		value, err := first.Do(context.Background(), "request")
		firstResult <- result{value: value, err: err}
	}()
	<-firstStarted
	go func() {
		value, err := second.Do(context.Background(), "request")
		secondResult <- result{value: value, err: err}
	}()

	// The legacy concatenated key is also observed here so the test knows the
	// second call has joined the colliding flight before releasing the leader.
	xtest.Eventually(t, time.Second, func() bool {
		select {
		case <-secondStarted:
			return true
		default:
			return coalescer.Waiters("foo:bar:baz") > 0
		}
	})
	close(releaseFirst)

	firstGot := <-firstResult
	secondGot := <-secondResult
	if firstGot.err != nil || firstGot.value != "first" {
		t.Fatalf("first result = (%q, %v), want (first, nil)", firstGot.value, firstGot.err)
	}
	if secondGot.err != nil || secondGot.value != "second" {
		t.Fatalf("second result = (%q, %v), want (second, nil)", secondGot.value, secondGot.err)
	}
}
