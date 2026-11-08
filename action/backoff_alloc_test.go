//go:build !race

package action

import (
	"testing"
	"time"
)

// Measure the production CSPRNG path without race instrumentation.
func TestExponentialJitterSecureSourceAllocatesNothing(t *testing.T) {
	jitter := ExponentialJitter(100*time.Millisecond, time.Second)
	var got time.Duration
	allocs := testing.AllocsPerRun(100, func() {
		got = jitter(8)
	})
	if allocs != 0 {
		t.Fatalf("jitter invocation allocated %.2f objects", allocs)
	}
	if got == 0 {
		t.Fatal("jitter invocation produced zero duration")
	}
}
