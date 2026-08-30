package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type WorkerReq struct {
	WorkerID int
	Task     string
}

func main() {
	var attempts atomic.Int32

	// Flaky Worker – fails on the 1st attempt for WorkerID=3 with a transient error,
	// then recovers on the 2nd attempt via RetryMiddleware.
	flakyWorker := action.New("flaky.worker", func(ctx context.Context, req WorkerReq) (string, error) {
		att := attempts.Add(1)
		if req.WorkerID == 3 && att == 1 {
			// Returns a transient error to trigger RetryMiddleware
			return "", xerr.Unavailable("temporary database connection glitch (transient)")
		}
		return fmt.Sprintf("Processed task %q by Worker %d", req.Task, req.WorkerID), nil
	}).
		Retry(2, action.ConstantBackoff(10*time.Millisecond)).
		Build()

	tasks := []WorkerReq{
		{WorkerID: 1, Task: "Risk Analysis"},
		{WorkerID: 2, Task: "Portfolio Valuation"},
		{WorkerID: 3, Task: "Credit Scoring"}, // Fails on attempt 1, recovers on attempt 2
		{WorkerID: 4, Task: "Compliance Check"},
		{WorkerID: 5, Task: "PDF Generation"},
	}

	ctx := context.Background()

	fmt.Println("🚀 Executing parallel FanOut with bounded concurrency (maxConcurrency = 2)...")
	results := action.FanOut(ctx, flakyWorker, tasks, 2)

	for i, r := range results {
		if r.Err != nil {
			fmt.Printf("❌ Task %d [%s] failed: %v\n", i+1, tasks[i].Task, r.Err)
		} else {
			fmt.Printf("✅ Result %d: %s\n", i+1, r.Value)
		}
	}
}
