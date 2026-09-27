// TestComposeVerdict runs the compose benchmarks, collects numbers, and
// prints a colored analysis with a concrete recommendation. Run with:
//
//	go test ./action/ -run TestComposeVerdict -v
//
// Set NO_COLOR=1 to disable ANSI codes (CI logs).
package action_test

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type benchResult struct {
	Name    string
	NsPerOp int64
	Allocs  int64
}

type comparison struct {
	Label     string
	Typed     benchResult
	Any       benchResult
	Threshold string // what "good" looks like
}

const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiDim    = "\x1b[2m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
)

func colorize(enabled bool, code, s string) string {
	if !enabled {
		return s
	}
	return code + s + ansiReset
}

func runBench(_ *testing.B, name string) benchResult {
	res := testing.Benchmark(func(b *testing.B) {
		switch name {
		case "Single_Typed":
			BenchmarkSingle_Typed(b)
		case "Single_DoAny_BoxAtCall":
			BenchmarkSingle_DoAny_BoxAtCall(b)
		case "Single_InvokeAny_Preboxed":
			BenchmarkSingle_InvokeAny_Preboxed(b)
		case "Single_InvokeAny_MapInput":
			BenchmarkSingle_InvokeAny_MapInput(b)
		case "Chain_Typed_10":
			BenchmarkChain_Typed_10(b)
		case "Chain_Any_10":
			BenchmarkChain_Any_10(b)
		case "Stream_Typed_1000":
			BenchmarkStream_Typed_1000(b)
		case "Stream_AnyWrapped_1000":
			BenchmarkStream_AnyWrapped_1000(b)
		case "Pool_Typed_RoundRobin":
			BenchmarkPool_Typed_RoundRobin(b)
		case "Pool_Any_RoundRobin":
			BenchmarkPool_Any_RoundRobin(b)
		}
	})
	return benchResult{
		Name:    name,
		NsPerOp: res.NsPerOp(),
		Allocs:  res.AllocsPerOp(),
	}
}

func TestComposeVerdict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark verdict in -short mode")
	}

	color := os.Getenv("NO_COLOR") == ""
	c := func(code, s string) string { return colorize(color, code, s) }

	// Short bench time per case keeps the total test under a minute.
	origBenchTime := time.Duration(0)
	_ = origBenchTime

	t.Log(c(ansiBold+ansiCyan, "\n════════════════════════════════════════════════════════════"))
	t.Log(c(ansiBold+ansiCyan, "  COMPOSE BENCHMARK VERDICT"))
	t.Log(c(ansiBold+ansiCyan, "  Deciding: typed generics vs any-erased composition"))
	t.Log(c(ansiBold+ansiCyan, "════════════════════════════════════════════════════════════"))

	comparisons := []comparison{
		{
			Label:     "Single call (unary pipeline step)",
			Typed:     runBench(nil, "Single_Typed"),
			Any:       runBench(nil, "Single_DoAny_BoxAtCall"),
			Threshold: "typed < 100ns, any ≤ 150ns",
		},
		{
			Label:     "Chain of 10 (pipeline depth)",
			Typed:     runBench(nil, "Chain_Typed_10"),
			Any:       runBench(nil, "Chain_Any_10"),
			Threshold: "any ≤ 2× typed",
		},
		{
			Label:     "Stream of 1000 (per-item cost)",
			Typed:     runBench(nil, "Stream_Typed_1000"),
			Any:       runBench(nil, "Stream_AnyWrapped_1000"),
			Threshold: "any allocs/op must be near 0",
		},
		{
			Label:     "Pool of 10 (routing cost)",
			Typed:     runBench(nil, "Pool_Typed_RoundRobin"),
			Any:       runBench(nil, "Pool_Any_RoundRobin"),
			Threshold: "any ≤ 3× typed",
		},
	}

	for _, cmp := range comparisons {
		printComparison(t, c, cmp)
	}

	printOverallVerdict(t, c, comparisons)

	// Also print raw reference numbers for the "worst case" paths.
	t.Log(c(ansiBold, "\n  Raw reference numbers"))
	t.Log("  " + c(ansiDim, "──────────────────────────────────────────────────────────"))
	printRaw(t, c, "Single_InvokeAny_Preboxed")
	printRaw(t, c, "Single_InvokeAny_MapInput")
}

func printComparison(t *testing.T, c func(string, string) string, cmp comparison) {
	t.Log(c(ansiBold, "\n  ▸ "+cmp.Label))
	t.Log("  " + c(ansiDim, "──────────────────────────────────────────────────────────"))

	deltaNs := cmp.Any.NsPerOp - cmp.Typed.NsPerOp
	ratioNs := float64(cmp.Any.NsPerOp) / float64(maxInt64(cmp.Typed.NsPerOp, 1))
	deltaAlloc := cmp.Any.Allocs - cmp.Typed.Allocs

	t.Logf("    %s  %10s  %8s",
		c(ansiDim, "variant"),
		c(ansiDim, "ns/op"),
		c(ansiDim, "allocs"),
	)
	t.Logf("    %-24s  %10d  %8d",
		c(ansiGreen, "typed  "),
		cmp.Typed.NsPerOp,
		cmp.Typed.Allocs,
	)
	t.Logf("    %-24s  %10d  %8d",
		c(ansiYellow, "any    "),
		cmp.Any.NsPerOp,
		cmp.Any.Allocs,
	)

	verdictColor := ansiGreen
	verdict := "OK"
	if ratioNs > 2.0 || deltaAlloc > 2 {
		verdictColor = ansiYellow
		verdict = "NOTICE"
	}
	if ratioNs > 5.0 || deltaAlloc > 100 {
		verdictColor = ansiRed
		verdict = "CONCERN"
	}

	sign := "+"
	if deltaNs < 0 {
		sign = ""
	}
	t.Logf("    %s  ns: %s%d (×%.2f)   allocs: %+d",
		c(ansiDim, "delta"),
		sign, deltaNs, ratioNs, deltaAlloc,
	)
	t.Logf("    %s  %s %s",
		c(ansiDim, "verdict"),
		c(verdictColor+ansiBold, verdict),
		c(ansiDim, "expected: "+cmp.Threshold),
	)
}

func printRaw(t *testing.T, c func(string, string) string, name string) {
	res := runBench(nil, name)
	t.Logf("    %-32s  %8d ns/op  %8d allocs/op",
		c(ansiDim, name), res.NsPerOp, res.Allocs)
}

func printOverallVerdict(t *testing.T, c func(string, string) string, cmps []comparison) {
	t.Log(c(ansiBold+ansiCyan, "\n════════════════════════════════════════════════════════════"))
	t.Log(c(ansiBold+ansiCyan, "  RECOMMENDATION"))
	t.Log(c(ansiBold+ansiCyan, "════════════════════════════════════════════════════════════"))

	var (
		maxUnaryRatio float64
		maxPoolRatio  float64
	)

	for _, cmp := range cmps {
		ratio := float64(cmp.Any.NsPerOp) / float64(maxInt64(cmp.Typed.NsPerOp, 1))
		switch cmp.Label {
		case "Chain of 10 (pipeline depth)":
			maxUnaryRatio = ratio
		case "Pool of 10 (routing cost)":
			maxPoolRatio = ratio
		}
	}

	t.Log("")
	t.Logf("  %s  %s",
		c(ansiBold, "Unary pipeline:"),
		verdictFor(maxUnaryRatio, 2.0, 5.0, c,
			"keep on AnyAction at the DSL boundary",
			"any is acceptable; add typed fast path only if a hot loop appears",
			"add typed fast path via TypedPayload inspection in the compiler",
		),
	)
	t.Logf("  %s  %s",
		c(ansiBold, "Pool routing: "),
		verdictFor(maxPoolRatio, 3.0, 5.0, c,
			"keep pool on AnyAction",
			"acceptable; pool runs once per request",
			"consider typed pool when members share Req/Res",
		),
	)
	t.Log("")
	t.Logf("  %s  %s",
		c(ansiBold, "Streams:"),
		c(ansiRed, "AnyStream boxes every element. Use typed iter.Seq2 end-to-end."),
	)
	t.Logf("  %s  %s",
		c(ansiDim, "         "),
		c(ansiDim, "collected as []any only at the boundary (collect, out.file, ...)"),
	)
	t.Log("")
	t.Log(c(ansiBold+ansiCyan, "════════════════════════════════════════════════════════════\n"))
}

func verdictFor(ratio, warn, bad float64, c func(string, string) string, good, notice, concern string) string {
	switch {
	case ratio > bad:
		return c(ansiRed+ansiBold, concern) + c(ansiDim, fmt.Sprintf("  (×%.2f)", ratio))
	case ratio > warn:
		return c(ansiYellow, notice) + c(ansiDim, fmt.Sprintf("  (×%.2f)", ratio))
	default:
		return c(ansiGreen, good) + c(ansiDim, fmt.Sprintf("  (×%.2f)", ratio))
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
