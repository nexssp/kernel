package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
)

func main() {
	coalescer := action.NewCoalescer()
	var dbHits atomic.Int32

	fetchExchangeRate := action.New("fx.get", func(_ context.Context, _ string) (float64, error) {
		dbHits.Add(1)
		time.Sleep(100 * time.Millisecond) // Heavy DB / API computation
		return 1.0850, nil
	}).
		Coalesce(coalescer, func(req string) string { return req }).
		Build()

	const concurrentCallers = 50
	var wg sync.WaitGroup
	wg.Add(concurrentCallers)

	fmt.Printf("⚡ Dispatching %d concurrent requests to the same resource...\n", concurrentCallers)
	startBarrier := make(chan struct{})

	for i := range concurrentCallers {
		go func(id int) {
			defer wg.Done()
			<-startBarrier

			rate, err := fetchExchangeRate.Do(context.Background(), "EUR_USD")
			if err != nil {
				fmt.Printf("Caller %d error: %v\n", id, err)
				return
			}
			if id == 0 {
				fmt.Printf("Exchange rate retrieved: %.4f\n", rate)
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	fmt.Printf("🎯 Actual DB/API hits: %d (Thundering Herd Shield: 100%% effective)\n", dbHits.Load())
}
