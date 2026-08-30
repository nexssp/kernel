package action_test

import (
	"math"
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

func TestConstantBackoff(t *testing.T) {
	c := action.ConstantBackoff(123 * time.Millisecond)
	for i := 1; i <= 3; i++ {
		if c(i) != 123*time.Millisecond {
			t.Fail()
		}
	}
}

func TestExponentialJitter(t *testing.T) {
	j := action.ExponentialJitter(100*time.Millisecond, 1*time.Second)
	base := 100 * time.Millisecond
	for i := 1; i <= 3; i++ {
		d := j(i)
		exp := min(time.Duration(float64(base)*math.Pow(2, float64(i-1))), 1*time.Second)
		minVal := time.Duration(float64(exp) * 0.7)
		maxVal := time.Duration(float64(exp) * 1.3)
		if d < minVal || d > maxVal {
			t.Errorf("jitter out of range: got %v, expected [%v, %v]", d, minVal, maxVal)
		}
	}
}
