package ringbuf

import "testing"

func TestNewBufferWithOptionsZeroSizeItemIsNotCappedByMemoryBudget(t *testing.T) {
	const capacity = 1_000_000
	const smallBudgetBytes = 128
	buffer, err := NewBufferWithOptions[struct{}](BufferOptions{
		Capacity:          capacity,
		MemoryBudgetBytes: smallBudgetBytes,
	})
	if err != nil {
		t.Fatalf("NewBufferWithOptions[struct{}](capacity=%d, budget=%d) error = %v", capacity, smallBudgetBytes, err)
	}
	if got := len(buffer.buf); got != capacity {
		t.Fatalf("logical buffer length = %d, want %d", got, capacity)
	}
	if got := cap(buffer.buf); got != capacity {
		t.Fatalf("logical buffer capacity = %d, want %d", got, capacity)
	}
}
