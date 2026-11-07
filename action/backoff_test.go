package action_test

import (
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
)

func TestExponentialBackoff(t *testing.T) {
	backoff := action.ExponentialBackoff(100*time.Millisecond, 2*time.Second)
	assert := func(attempt int, expected time.Duration) {
		got := backoff(attempt)
		if got != expected {
			t.Errorf("attempt %d: expected %v, got %v", attempt, expected, got)
		}
	}
	assert(1, 100*time.Millisecond)
	assert(2, 200*time.Millisecond)
	assert(3, 400*time.Millisecond)
	// capped at 2s
	assert(10, 2*time.Second)
}

func TestLinearBackoff(t *testing.T) {
	l := action.LinearBackoff(50 * time.Millisecond)
	if l(1) != 50*time.Millisecond || l(2) != 100*time.Millisecond || l(3) != 150*time.Millisecond {
		t.Fail()
	}
}

func TestLinearBackoffSaturatesOverflow(t *testing.T) {
	const (
		maxDuration = time.Duration(1<<63 - 1)
		minDuration = time.Duration(-1 << 63)
	)
	tests := []struct {
		name string
		step time.Duration
		try  int
		want time.Duration
	}{
		{name: "positive boundary", step: maxDuration / 2, try: 2, want: maxDuration - 1},
		{name: "positive overflow", step: maxDuration / 2, try: 3, want: maxDuration},
		{name: "negative boundary", step: minDuration, try: 1, want: minDuration},
		{name: "negative step overflow", step: minDuration, try: 2, want: minDuration},
		{name: "negative attempt overflow", step: maxDuration, try: -2, want: minDuration},
		{name: "positive product from negatives", step: minDuration, try: -1, want: maxDuration},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := action.LinearBackoff(test.step)(test.try); got != test.want {
				t.Fatalf("LinearBackoff(%d)(%d) = %d, want %d", test.step, test.try, got, test.want)
			}
		})
	}
}

func TestConstantBackoff(t *testing.T) {
	c := action.ConstantBackoff(123 * time.Millisecond)
	for i := 1; i <= 3; i++ {
		if c(i) != 123*time.Millisecond {
			t.Fail()
		}
	}
}

func TestExponentialJitter(t *testing.T) {
	const (
		base       = 100 * time.Millisecond
		maxDur     = 1 * time.Second
		iterations = 50
	)
	jitterFn := action.ExponentialJitter(base, maxDur)

	// 1. Verify strict bounds across attempts, including cap and edge cases
	testCases := []struct {
		name    string
		attempt int
	}{
		{"zero attempt", 0},
		{"first attempt", 1},
		{"mid attempt", 3},
		{"at cap attempt", 10},
		{"overflow attempt", 65},
		{"high attempt", 100},
	}

	for _, tc := range testCases {
		seenValues := make(map[time.Duration]struct{}, iterations)

		for range iterations {
			val := jitterFn(tc.attempt)

			if val < base {
				t.Fatalf("%s (attempt %d): returned %v below base %v", tc.name, tc.attempt, val, base)
			}
			if val > maxDur {
				t.Fatalf("%s (attempt %d): returned %v exceeding max %v", tc.name, tc.attempt, val, maxDur)
			}

			seenValues[val] = struct{}{}
		}

		// 2. Verify non-determinism: jitter must produce varying durations
		if tc.attempt > 0 && len(seenValues) == 1 {
			t.Errorf("%s (attempt %d): jitter is deterministic, produced constant value %v", tc.name, tc.attempt, jitterFn(tc.attempt))
		}
	}
}
