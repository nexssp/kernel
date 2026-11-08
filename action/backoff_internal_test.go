package action

import (
	"testing"
	"time"
)

func TestExponentialJitterUsesInjectedSamples(t *testing.T) {
	samples := [...]float64{0, 0.5, 1}
	index := 0
	jitter := exponentialJitterWithSource(100*time.Millisecond, time.Second, func() float64 {
		sample := samples[index]
		index++
		return sample
	})
	want := [...]time.Duration{140 * time.Millisecond, 200 * time.Millisecond, 260 * time.Millisecond}
	for i, expected := range want {
		if got := jitter(2); got != expected {
			t.Fatalf("jitter sample %v = %v, want %v", samples[i], got, expected)
		}
	}
	if index != len(samples) {
		t.Fatalf("sample source called %d times, want %d", index, len(samples))
	}
}

func TestExponentialJitterInjectedSourceAllocatesNothing(t *testing.T) {
	jitter := exponentialJitterWithSource(100*time.Millisecond, time.Second, func() float64 { return 0.5 })
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

func TestJitterDurationVariesWithDeterministicSamples(t *testing.T) {
	const (
		base        = 100 * time.Millisecond
		maximum     = time.Second
		exponential = 200 * time.Millisecond
	)
	tests := []struct {
		name   string
		sample float64
		want   time.Duration
	}{
		{name: "minimum sample", sample: 0, want: 140 * time.Millisecond},
		{name: "middle sample", sample: 0.5, want: 200 * time.Millisecond},
		{name: "maximum sample", sample: 1, want: 260 * time.Millisecond},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := jitterDuration(exponential, base, maximum, test.sample); got != test.want {
				t.Fatalf("jitterDuration(%v, %v, %v, %v) = %v, want %v",
					exponential, base, maximum, test.sample, got, test.want)
			}
		})
	}
}
