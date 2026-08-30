package main

import (
	"context"
	"fmt"

	"github.com/nexssp/kernel/action"
)

func main() {
	ctx := context.Background()

	// Step 1: Parse string to integer length
	step1 := action.New("step.length", func(_ context.Context, text string) (int, error) {
		return len(text), nil
	}).Build()

	// Step 2: Double the integer
	step2 := action.New("step.double", func(_ context.Context, count int) (int, error) {
		return count * 2, nil
	}).Build()

	// Pipeline: String -> Int -> Int (A -> B)
	pipeline := action.Pipe("text.pipeline", step1, step2).Build()

	result, err := pipeline.Do(ctx, "Hello Nexss")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Pipe Result ('Hello Nexss' length * 2) = %d\n", result)

	// Concurrent FanOut: Process 5 requests with max 2 running concurrently
	multiplyAct := action.New("multiply.action", func(_ context.Context, n int) (int, error) {
		return n * 10, nil
	}).Build()

	requests := []int{1, 2, 3, 4, 5}
	results := action.FanOut(ctx, multiplyAct, requests, 2) // Max concurrency: 2

	fmt.Println("\nFanOut Results:")
	for i, r := range results {
		fmt.Printf("Input: %d -> Output: %d (Err: %v)\n", requests[i], r.Value, r.Err)
	}
}
