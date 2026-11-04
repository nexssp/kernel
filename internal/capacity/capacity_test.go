package capacity_test

import (
	"testing"

	"github.com/nexssp/kernel/internal/capacity"
)

func TestPowerOfTwoCeiling(t *testing.T) {
	tests := []struct {
		requested int
		maximum   int
		want      int
		ok        bool
	}{
		{requested: 0, maximum: 8, want: 2, ok: true},
		{requested: 2, maximum: 8, want: 2, ok: true},
		{requested: 3, maximum: 8, want: 4, ok: true},
		{requested: 8, maximum: 8, want: 8, ok: true},
		{requested: 9, maximum: 8, ok: false},
		{requested: 1, maximum: 3, ok: false},
	}

	for _, test := range tests {
		got, ok := capacity.PowerOfTwoCeiling(test.requested, test.maximum)
		if got != test.want || ok != test.ok {
			t.Errorf("PowerOfTwoCeiling(%d, %d) = (%d, %t), want (%d, %t)", test.requested, test.maximum, got, ok, test.want, test.ok)
		}
	}
}

func TestPowerOfTwoCeilingRejectsMaximumInteger(t *testing.T) {
	maximumInteger := int(^uint(0) >> 1)
	if got, ok := capacity.PowerOfTwoCeiling(maximumInteger, capacity.MaxPowerOfTwo); ok {
		t.Fatalf("PowerOfTwoCeiling(max int, max capacity) = %d, want rejection", got)
	}
}
