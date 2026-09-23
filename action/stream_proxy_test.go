package action_test

import (
	"context"
	"iter"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestStreamProxy_Uninitialized(t *testing.T) {
	p := action.NewStreamProxy(nil)
	if p.Current() != nil || p.ReqPayload() != nil || p.ResPayload() != nil {
		t.Fatal("uninitialized proxy exposed an active stream")
	}
	if _, err := p.DoStreamAny(context.Background(), nil); err == nil {
		t.Fatal("expected uninitialized proxy error")
	}
}

func TestStreamProxy_SwapAndSnapshot(t *testing.T) {
	first := action.NewStream("first", func(context.Context, struct{}) (iter.Seq2[string, error], error) {
		return func(yield func(string, error) bool) { yield("first", nil) }, nil
	})
	second := action.NewStream("second", func(context.Context, struct{}) (iter.Seq2[string, error], error) {
		return func(yield func(string, error) bool) { yield("second", nil) }, nil
	})

	p := action.NewStreamProxy(first)
	before, err := p.DoStreamAny(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	p.Swap(second)

	if got := p.Describe().Name; got != "second" {
		t.Fatalf("active name=%q, want second", got)
	}
	var beforeItem string
	before(func(item any, err error) bool {
		if err != nil {
			t.Fatal(err)
		}
		value, ok := item.(string)
		if !ok {
			t.Errorf("item type=%T, want string", item)
			return false
		}
		beforeItem = value
		return true
	})
	if beforeItem != "first" {
		t.Fatalf("snapshot item=%q, want first", beforeItem)
	}

	after, err := p.DoStreamAny(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	var afterItem string
	after(func(item any, err error) bool {
		if err != nil {
			t.Fatal(err)
		}
		value, ok := item.(string)
		if !ok {
			t.Errorf("item type=%T, want string", item)
			return false
		}
		afterItem = value
		return true
	})
	if afterItem != "second" {
		t.Fatalf("active item=%q, want second", afterItem)
	}
}

func TestStreamProxy_SwapNilKeepsActive(t *testing.T) {
	stream := action.NewStream("stable", func(context.Context, struct{}) (iter.Seq2[int, error], error) {
		return func(yield func(int, error) bool) { yield(1, nil) }, nil
	})
	p := action.NewStreamProxy(stream)
	p.Swap(nil)
	if p.Current() == nil || p.Describe().Name != "stable" {
		t.Fatal("nil swap disabled the active stream")
	}
	if _, err := p.DoStreamAny(context.Background(), struct{}{}); err != nil {
		t.Fatal(err)
	}
}
