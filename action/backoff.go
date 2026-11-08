// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

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
	base, maxDuration = normalizedBackoffBounds(base, maxDuration)
	return func(attempt int) time.Duration {
		if attempt <= 1 || base == 0 {
			return base
		}
		delay := base
		for i := 1; i < attempt; i++ {
			if delay > maxDuration/2 {
				return maxDuration
			}
			delay *= 2
			if delay >= maxDuration {
				return maxDuration
			}
		}
		return delay
	}
}

func normalizedBackoffBounds(base, maxDuration time.Duration) (normalizedBase, normalizedMaximum time.Duration) {
	if base < 0 {
		base = 0
	}
	if maxDuration < 0 {
		maxDuration = 0
	}
	if base > maxDuration {
		base = maxDuration
	}
	return base, maxDuration
}

// ExponentialJitter adds ±30% random jitter to ExponentialBackoff.
// Prevents thundering-herd on simultaneous retries.
//
// The result is always capped at maxDuration. Jitter is applied to
// the exponential value and then the whole result is clamped.
//
// Usage: .Retry(3, action.ExponentialJitter(100*time.Millisecond, 5*time.Second))
func ExponentialJitter(base, maxDuration time.Duration) func(attempt int) time.Duration {
	return exponentialJitterWithSource(base, maxDuration, secureJitterFloat)
}

func exponentialJitterWithSource(base, maxDuration time.Duration, sample func() float64) func(attempt int) time.Duration {
	base, maxDuration = normalizedBackoffBounds(base, maxDuration)
	exp := ExponentialBackoff(base, maxDuration)
	return func(attempt int) time.Duration {
		return jitterDuration(exp(attempt), base, maxDuration, sample())
	}
}

// jitterDuration applies ±30% jitter using a normalized sample in [0,1] and
// clamps the result to [base, maxDuration]. Production callers use
// secureJitterFloat to provide the sample.
func jitterDuration(duration, base, maxDuration time.Duration, sample float64) time.Duration {
	d := float64(duration)
	result := d + d*0.3*(sample*2-1)
	switch {
	case result <= float64(base):
		return base
	case result >= float64(maxDuration):
		return maxDuration
	default:
		return time.Duration(result)
	}
}

// LinearBackoff increments by step each attempt.
// 100ms → 200ms → 300ms … Multiplication saturates on overflow.
func LinearBackoff(step time.Duration) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		multiplier := time.Duration(attempt)
		const (
			maxDuration = time.Duration(1<<63 - 1)
			minDuration = time.Duration(-1 << 63)
		)
		switch {
		case step > 0 && multiplier > 0 && step > maxDuration/multiplier:
			return maxDuration
		case step > 0 && multiplier < 0 && multiplier < minDuration/step:
			return minDuration
		case step < 0 && multiplier > 0 && step < minDuration/multiplier:
			return minDuration
		case step < 0 && multiplier < 0 && step < maxDuration/multiplier:
			return maxDuration
		default:
			//nolint:durationcheck // The bounds above prove this product is representable.
			return step * multiplier
		}
	}
}

// ConstantBackoff waits the same duration between every attempt.
func ConstantBackoff(d time.Duration) func(attempt int) time.Duration {
	return func(_ int) time.Duration { return d }
}
