package xtest

import "testing"

func AllocsPerRun(runs int, fn func()) float64 {
	return testing.AllocsPerRun(runs, fn)
}

func RequireZeroAlloc(tb testing.TB, runs int, fn func()) {
	tb.Helper()
	allocs := testing.AllocsPerRun(runs, fn)
	if allocs != 0 {
		tb.Fatalf("expected 0 allocations/op, got %.2f", allocs)
	}
}

func RequireMaxAlloc(tb testing.TB, runs int, limit float64, fn func()) {
	tb.Helper()
	allocs := testing.AllocsPerRun(runs, fn)
	if allocs > limit {
		tb.Fatalf("expected at most %.2f allocations/op, got %.2f", limit, allocs)
	}
}

func RequireAllocsBetween(tb testing.TB, runs int, minimum, maximum float64, fn func()) {
	tb.Helper()
	allocs := testing.AllocsPerRun(runs, fn)
	if allocs < minimum || allocs > maximum {
		tb.Fatalf("expected between %.2f and %.2f allocations/op, got %.2f", minimum, maximum, allocs)
	}
}
