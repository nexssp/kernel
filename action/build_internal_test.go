// build_internal_test.go
package action

import (
	"context"
	"testing"
)

func TestBuiltAction_ClaimRegistryHooksIsOneShot(t *testing.T) {
	act := New("task.run", func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}).Build()

	if !act.claimRegistryHooks() {
		t.Fatal("first claim must succeed")
	}
	if act.claimRegistryHooks() {
		t.Fatal("second claim must fail")
	}
}
