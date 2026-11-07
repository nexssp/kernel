// Package broadcast provides a concurrent-producer, multi-consumer ring buffer
// for broadcasting state snapshots.
package broadcast

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/nexssp/kernel/internal/capacity"
)

type slot[T any] struct {
	seq atomic.Uint64
	mu  sync.Mutex
	val T
}

// Topic is a multi-consumer ring buffer with serialized concurrent publishers.
// Consumers maintain private cursors.
type Topic[T any] struct {
	_      [56]byte
	head   atomic.Uint64
	_      [56]byte
	pubMu  sync.Mutex
	mask   uint64
	buffer []slot[T]
}

// NewTopic creates a topic with a power-of-two capacity. It returns nil when
// the requested capacity exceeds the supported maximum; use NewTopicChecked
// when the caller needs the error.
func NewTopic[T any](requested int) *Topic[T] {
	topic, err := NewTopicChecked[T](requested)
	if err != nil {
		return nil
	}
	return topic
}

// NewTopicChecked creates a topic and reports unsupported capacities.
func NewTopicChecked[T any](requested int) (*Topic[T], error) {
	if requested > capacity.MaxPowerOfTwo {
		return nil, fmt.Errorf("broadcast: capacity %d exceeds maximum %d", requested, capacity.MaxPowerOfTwo)
	}
	cap2, ok := capacity.PowerOfTwoCeiling(requested, capacity.MaxPowerOfTwo)
	if !ok {
		return nil, fmt.Errorf("broadcast: invalid capacity %d", requested)
	}

	t := &Topic[T]{
		mask:   uint64(cap2 - 1), //nolint:gosec // cap2 >= 2 and positive, fits in uint64
		buffer: make([]slot[T], cap2),
	}
	for i := range t.buffer {
		t.buffer[i].seq.Store(math.MaxUint64) // Sentinel: uninitialized
	}
	return t, nil
}

// Publish stores a message and increments the sequence. Concurrent publishers
// are serialized; consumers may read concurrently with publishers.
func (t *Topic[T]) Publish(msg T) {
	t.pubMu.Lock()
	defer t.pubMu.Unlock()

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
	// A full buffer remains readable until the producer advances head again.
	bufferCapacity := t.mask + 1
	if head-tail > bufferCapacity {
		tail = head - bufferCapacity
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
