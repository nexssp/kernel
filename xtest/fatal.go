package xtest

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// ExpectFatal isolates subprocess crash testing. It runs fn in a child process
// and asserts that it fails with wantSubstr in its output.
// It is used to test test-helpers (verifying that they correctly call t.Fatal).
func ExpectFatal(t *testing.T, testName, envVar, wantSubstr string, fn func(t *testing.T)) {
	t.Helper()
	if os.Getenv(envVar) == "1" {
		fn(t)
		return
	}

	// The test binary path and arguments are controlled by the test itself.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+testName+"$", "-test.v") //nolint:gosec // trusted test binary
	cmd.Env = append(os.Environ(), envVar+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected subprocess to fail; output:\n%s", out)
	}
	if !strings.Contains(string(out), wantSubstr) {
		t.Fatalf("expected %q in subprocess output, got:\n%s", wantSubstr, out)
	}
}
