package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type StockRequest struct {
	SKU string
}

type StockResponse struct {
	SKU      string
	Quantity int
}

func main() {
	var databaseCalls atomic.Int32

	// Simulated flaky stock checker
	checkStock := action.New("inventory.check", func(ctx context.Context, req StockRequest) (StockResponse, error) {
		call := databaseCalls.Add(1)
		if call == 1 {
			// First call fails with a transient error
			return StockResponse{}, xerr.Unavailable("database connection glitch")
		}
		return StockResponse{SKU: req.SKU, Quantity: 150}, nil
	}).
		// 1. Timeout protection
		Timeout(2*time.Second).
		// 2. Retry with Exponential Backoff and 30% Jitter
		Retry(2, action.ExponentialJitter(20*time.Millisecond, 200*time.Millisecond)).
		// 3. Multi-layer in-memory Cache (5-minute TTL)
		Cache(5*time.Minute, func(r StockRequest) string { return "stock:" + r.SKU }).
		// 4. In-flight Singleflight Deduplication (Thundering Herd shield)
		Dedup(func(r StockRequest) string { return r.SKU }).
		// 5. Audit & Log Hooks
		HookExecuted(func(ctx context.Context, req StockRequest, res StockResponse, meta *action.Meta) {
			fmt.Printf("✅ [Audit] %s -> SKU: %s has %d items in stock\n", meta.Name, res.SKU, res.Quantity)
		}).
		Build()

	ctx := context.Background()

	// First execution (triggers retry, succeeds, and populates cache)
	res, err := checkStock.Do(ctx, StockRequest{SKU: "PROD-999"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("1st Call Result: %+v\n", res)

	// Second execution (served directly from cache in 0ms)
	cachedRes, err := checkStock.Do(ctx, StockRequest{SKU: "PROD-999"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("2nd Call Result (from cache): %+v\n", cachedRes)
}
