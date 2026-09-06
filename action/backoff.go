package action

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"time"
)

// secureJitterFloat returns a cryptographically secure random float in [0,1].
func secureJitterFloat() float64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0.5 // extremely unlikely, but prevents panic on error
	}
	val := binary.LittleEndian.Uint64(b[:])
	return float64(val) / float64(math.MaxUint64)
}

// ExponentialBackoff returns a backoff that doubles each attempt, capped at max.
// 100ms → 200ms → 400ms → 800ms … → max
//
// Usage: .Retry(3, action.ExponentialBackoff(100*time.Millisecond, 5*time.Second))
func ExponentialBackoff(base, maxDuration time.Duration) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		d := time.Duration(float64(base) * math.Pow(2, float64(attempt-1)))
		if d > maxDuration {
			return maxDuration
		}
		return d
	}
}

// ExponentialJitter adds ±30% random jitter to ExponentialBackoff.
// Prevents thundering-herd on simultaneous retries.
//
// Usage: .Retry(3, action.ExponentialJitter(100*time.Millisecond, 5*time.Second))
func ExponentialJitter(base, maxDuration time.Duration) func(attempt int) time.Duration {
	exp := ExponentialBackoff(base, maxDuration)
	return func(attempt int) time.Duration {
		d := float64(exp(attempt))
		// ±30% jitter
		jitter := d * 0.3 * (secureJitterFloat()*2 - 1)
		result := time.Duration(d + jitter)
		if result < 0 {
			return base
		}
		return result
	}
}

// LinearBackoff increments by step each attempt.
// 100ms → 200ms → 300ms …
func LinearBackoff(step time.Duration) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		return step * time.Duration(attempt)
	}
}

// ConstantBackoff waits the same duration between every attempt.
func ConstantBackoff(d time.Duration) func(attempt int) time.Duration {
	return func(_ int) time.Duration { return d }
}
