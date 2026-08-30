package main

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/nexssp/kernel/action"
)

type StreamQuery struct {
	BatchSize int
}

type Record struct {
	ID        int
	Timestamp time.Time
}

func main() {
	// Define stream with Go 1.23+ iter.Seq2
	streamAction := action.NewStream("telemetry.stream", func(ctx context.Context, req StreamQuery) (iter.Seq2[Record, error], error) {
		return func(yield func(Record, error) bool) {
			for i := 1; i <= req.BatchSize; i++ {
				// React immediately to context cancellation (e.g., client disconnect)
				if ctx.Err() != nil {
					yield(Record{}, ctx.Err())
					return
				}

				rec := Record{ID: i, Timestamp: time.Now().UTC()}

				// Yield delivers item immediately (zero buffer in RAM)
				if !yield(rec, nil) {
					return // Consumer broke out of iteration
				}
				time.Sleep(10 * time.Millisecond) // Simulated stream delay
			}
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	fmt.Println("🌊 Consuming data stream with constant memory O(1)...")
	seq, err := streamAction.Do(ctx, StreamQuery{BatchSize: 100})
	if err != nil {
		panic(err)
	}

	for item, err := range seq {
		if err != nil {
			fmt.Println("🛑 Stream stopped on timeout:", err)
			break
		}
		fmt.Printf("📦 Received packet #%d at: %s\n", item.ID, item.Timestamp.Format("15:04:05.000"))
	}
}
