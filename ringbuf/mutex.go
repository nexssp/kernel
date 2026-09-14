package ringbuf

import "sync"

// Buffer is a simple mutex-guarded ring buffer for T.
type Buffer[T any] struct {
	mu     sync.Mutex
	buf    []T
	pos    int
	filled bool
}

func NewBuffer[T any](capacity int) *Buffer[T] {
	if capacity < 1 {
		capacity = 1
	}
	return &Buffer[T]{buf: make([]T, capacity)}
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
