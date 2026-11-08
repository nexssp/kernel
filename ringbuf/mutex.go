package ringbuf

import (
	"sync"
	"unsafe"
)

// Buffer is a simple mutex-guarded ring buffer for T.
type Buffer[T any] struct {
	mu     sync.Mutex
	buf    []T
	pos    int
	filled bool
}

// BufferOptions configure one generic Buffer allocation. Capacity below 1
// retains NewBuffer's historical normalization to 1. MemoryBudgetBytes covers
// the Buffer header and backing array, excluding allocator metadata and memory
// reachable through pointers stored in T.
type BufferOptions struct {
	Capacity          int
	MemoryBudgetBytes uint64
}

// NewBuffer allocates a Buffer using the default 64 MiB per-buffer budget. It
// returns nil when the allocation would exceed that budget; use
// NewBufferChecked to inspect the error or NewBufferWithOptions for an explicit
// per-buffer override. Capacities below 1 are normalized to 1.
func NewBuffer[T any](capacity int) *Buffer[T] {
	buffer, err := NewBufferChecked[T](capacity)
	if err != nil {
		return nil
	}
	return buffer
}

// NewBufferChecked allocates a Buffer using DefaultMemoryBudgetBytes and
// returns an error before allocating when the estimated size exceeds the
// budget or the backing array size is not representable.
func NewBufferChecked[T any](capacity int) (*Buffer[T], error) {
	return NewBufferWithOptions[T](BufferOptions{
		Capacity:          capacity,
		MemoryBudgetBytes: DefaultMemoryBudgetBytes,
	})
}

// NewBufferWithOptions allocates a Buffer after checking the estimated size
// against the explicitly selected per-buffer budget. A zero budget rejects
// every allocation. The estimate excludes allocator metadata and objects
// reachable through pointers stored in T. For zero-size T, no per-element bytes
// are counted, so the byte budget does not cap logical capacity.
func NewBufferWithOptions[T any](options BufferOptions) (*Buffer[T], error) {
	capacity := max(options.Capacity, 1)

	var zero T
	itemSize := uint64(unsafe.Sizeof(zero))
	maxInt := uint64(^uint(0) >> 1)
	if itemSize != 0 && uint64(capacity) > maxInt/itemSize {
		return nil, ErrInvalidCapacity
	}

	backingBytes := uint64(capacity) * itemSize
	headerBytes := uint64(unsafe.Sizeof(Buffer[T]{}))
	if backingBytes > ^uint64(0)-headerBytes {
		return nil, ErrInvalidCapacity
	}
	if headerBytes+backingBytes > options.MemoryBudgetBytes {
		return nil, ErrMemoryBudgetExceeded
	}

	return &Buffer[T]{buf: make([]T, capacity)}, nil
}

func (b *Buffer[T]) Push(item T) {
	b.mu.Lock()
	b.buf[b.pos] = item
	b.pos = (b.pos + 1) % len(b.buf)
	if b.pos == 0 {
		b.filled = true
	}
	b.mu.Unlock()
}

func (b *Buffer[T]) Snapshot() []T {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.filled {
		out := make([]T, b.pos)
		copy(out, b.buf[:b.pos])
		return out
	}

	out := make([]T, len(b.buf))
	copied := copy(out, b.buf[b.pos:])
	copy(out[copied:], b.buf[:b.pos])
	return out
}
