package ktest_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xtest/ktest"
)

type dummyRoute struct {
	Method string
	Path   string
}

type ValidPayload struct {
	Name string `json:"name"`
}

func TestAssertContracts_ValidActionsPass(t *testing.T) {
	t.Parallel()

	act1 := action.New("user.create", func(_ context.Context, _ ValidPayload) (string, error) {
		return "ok", nil
	}).Route(dummyRoute{"POST", "/users"}).Build()

	act2 := action.New("user.get", func(_ context.Context, _ ValidPayload) (string, error) {
		return "ok", nil
	}).Route(dummyRoute{"GET", "/users"}).Build()

	ktest.AssertContracts(t, []action.AnyAction{act1, act2})
}
