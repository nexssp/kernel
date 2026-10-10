// Package broadcast provides a lock-free Single-Producer Multi-Consumer (SPMC)
// ring buffer for broadcasting state snapshots without blocking the producer.
package broadcast

import (
	"math"
	"sync/atomic"
)

type slot[T any] struct {
	seq atomic.Uint64
	val T
}

// Topic is a lock-free, SPMC ring buffer.
// Producer writes sequentially; consumers maintain private cursors.
type Topic[T any] struct {
	_      [56]byte
	head   atomic.Uint64
	_      [56]byte
	mask   uint64
	buffer []slot[T]
}

// NewTopic creates a topic with a power-of-two capacity.
func NewTopic[T any](capacity int) *Topic[T] {
	cap2 := 2
	for cap2 < capacity {
		cap2 <<= 1
	}

	t := &Topic[T]{
		mask:   uint64(cap2 - 1), //nolint:gosec // cap2 >= 2 and positive, fits in uint64
		buffer: make([]slot[T], cap2),
	}
	for i := range t.buffer {
		t.buffer[i].seq.Store(math.MaxUint64) // Sentinel: uninitialized
	}
	return t
}

// Publish stores a message and increments the sequence.
// Concurrency: strictly single-producer.
func (t *Topic[T]) Publish(msg T) {
	seq := t.head.Load()
	s := &t.buffer[seq&t.mask]

	s.val = msg
	s.seq.Store(seq) // Establish happens-before boundary before publishing head
	t.head.Store(seq + 1)
}

// Cursor tracks a single consumer's read position.
type Cursor struct {
	Seq uint64
}

// Read copies available messages into dst. Returns the number of items read.
// If the consumer falls behind the buffer capacity, it is fast-forwarded to the
// oldest available surviving message to prevent stale reads or buffer overrun.
func (t *Topic[T]) Read(c *Cursor, dst []T) int {
	head := t.head.Load()
	tail := c.Seq

	if tail >= head {
		return 0
	}

	// The writer is actively writing at slot (head & mask).
	// The maximum safe unread window is t.mask (capacity - 1) items.
	if head-tail > t.mask {
		tail = head - t.mask
	}

	n := 0
	for tail < head && n < len(dst) {
		s := &t.buffer[tail&t.mask]
		// If the slot sequence does not match tail, the producer either
		// hasn't finished writing or has already lapped this slot.
		if s.seq.Load() != tail {
			break
		}
		dst[n] = s.val
		tail++
		n++
	}
	c.Seq = tail
	return n
}
