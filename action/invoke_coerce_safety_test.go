package action_test

import (
	"fmt"
	"testing"

	"github.com/nexssp/kernel/action"
)

type coerceCycleNode struct {
	Next *coerceCycleNode
}

func TestCoerce_IncompatibleNonEmptyInterfaceReturnsError(t *testing.T) {
	if _, err := action.Coerce[fmt.Stringer](123); err == nil {
		t.Fatal("Coerce[fmt.Stringer](123) returned no error")
	}
}

func TestCoerce_CyclicMapReturnsError(t *testing.T) {
	input := map[string]any{}
	input["Next"] = input
	if _, err := action.Coerce[coerceCycleNode](input); err == nil {
		t.Fatal("cyclic map input returned no error")
	}
}
