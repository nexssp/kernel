// Package broadcast provides a Single-Producer Multi-Consumer (SPMC)
// ring buffer for broadcasting state snapshots without blocking the producer.
package broadcast

import (
	"math"
	"sync"
	"sync/atomic"
)

type slot[T any] struct {
	seq atomic.Uint64
	mu  sync.Mutex
	val T
}

// Topic is an SPMC ring buffer.
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

	s.mu.Lock()
	s.val = msg
	s.seq.Store(seq) // Establish happens-before boundary before unlocking
	s.mu.Unlock()

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

		// 1. Optimistic lock-free check. If the sequence doesn't match,
		// the producer has lapped us or hasn't finished writing.
		if s.seq.Load() != tail {
			break
		}

		// 2. Lock to prevent torn reads and satisfy the Go Race Detector.
		s.mu.Lock()

		// 3. Double-check inside the lock. The producer might have
		// overwritten this slot in the microsecond we waited for the lock!
		if s.seq.Load() != tail {
			s.mu.Unlock()
			break
		}
		dst[n] = s.val
		s.mu.Unlock()

		tail++
		n++
	}
	c.Seq = tail
	return n
}
