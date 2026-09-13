package node

import (
	"os"
	"strings"
)

const Env = "NEXSS_NODE"

// Active returns the trimmed deployment node name (e.g. "gateway", "worker").
// An empty string denotes Monolith Mode.
func Active() string {
	return strings.TrimSpace(os.Getenv(Env))
}
