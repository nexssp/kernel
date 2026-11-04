// Package capacity contains internal size helpers shared by bounded kernel buffers.
package capacity

const MaxPowerOfTwo = 1 << 30

// PowerOfTwoCeiling returns the smallest power of two not less than requested.
// It reports false when maximum is invalid or requested exceeds maximum.
func PowerOfTwoCeiling(requested, maximum int) (int, bool) {
	if maximum < 2 || maximum&(maximum-1) != 0 || requested > maximum {
		return 0, false
	}
	if requested < 2 {
		return 2, true
	}

	capacity := 2
	for capacity < requested {
		if capacity > maximum/2 {
			return 0, false
		}
		capacity <<= 1
	}
	return capacity, true
}
