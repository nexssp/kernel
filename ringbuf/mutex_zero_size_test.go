package ringbuf

import "testing"

func TestNewBufferZeroSizeItemIsNotCappedByMemoryBudget(t *testing.T) {
	const capacity = 1_000_000
	buffer, err := NewBufferChecked[struct{}](capacity)
	if err != nil {
		t.Fatalf("NewBufferChecked[struct{}](%d) error = %v", capacity, err)
	}
	if got := len(buffer.buf); got != capacity {
		t.Fatalf("logical buffer length = %d, want %d", got, capacity)
	}
	if got := cap(buffer.buf); got != capacity {
		t.Fatalf("logical buffer capacity = %d, want %d", got, capacity)
	}
}
