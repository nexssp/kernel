package node_test

import (
	"testing"

	"github.com/nexssp/kernel/node"
)

func TestActive(t *testing.T) {
	t.Run("unset returns empty", func(t *testing.T) {
		t.Setenv(node.Env, "")
		if got := node.Active(); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})

	t.Run("set returns value", func(t *testing.T) {
		t.Setenv(node.Env, "gateway")
		if got := node.Active(); got != "gateway" {
			t.Fatalf("got %q, want gateway", got)
		}
	})

	t.Run("whitespace is trimmed", func(t *testing.T) {
		t.Setenv(node.Env, "  worker  ")
		if got := node.Active(); got != "worker" {
			t.Fatalf("got %q, want worker", got)
		}
	})
}
