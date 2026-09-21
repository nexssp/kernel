package ktest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestFake_ReturnsValue(t *testing.T) {
	act := ktest.Fake("fake", "hello").(*action.BuiltAction[any, any]) //nolint:forcetypeassert // Fake guarantees this type
	res, err := act.Do(context.Background(), struct{}{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if res != "hello" {
		t.Fatalf("res = %v, want hello", res)
	}
}

func TestFakeErr_ReturnsError(t *testing.T) {
	want := errors.New("boom")
	act := ktest.FakeErr("fake", want).(*action.BuiltAction[any, any]) //nolint:forcetypeassert // FakeErr guarantees this type
	_, err := act.Do(context.Background(), struct{}{})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestFakeSeq_Cycles(t *testing.T) {
	act := ktest.FakeSeq("seq", 1, 2, 3).(*action.BuiltAction[any, any]) //nolint:forcetypeassert // FakeSeq guarantees this type
	ctx := context.Background()

	for i, want := range []int{1, 2, 3, 3, 3} {
		got, err := act.Do(ctx, struct{}{})
		if err != nil {
			t.Fatalf("call %d err = %v", i, err)
		}
		if got != want {
			t.Fatalf("call %d: got %v, want %v", i, got, want)
		}
	}
}

func TestFakeSeq_EmptyPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	ktest.FakeSeq("seq")
}
