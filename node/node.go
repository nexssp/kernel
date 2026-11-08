// Package node resolves the active deployment node name from the environment.
//
// A node name such as "gateway" or "worker" lets one binary host a subset of
// actions per deployment role. An empty name means Monolith Mode, where every
// action is active.
package node

import (
	"os"
	"strings"
)

// Env is the environment variable that holds the active node name.
const Env = "NEXSS_NODE"

// Active returns the trimmed deployment node name (e.g. "gateway", "worker").
// An empty string denotes Monolith Mode.
func Active() string {
	return strings.TrimSpace(os.Getenv(Env))
}
