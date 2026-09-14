// package ringbuf
// Cache-line-aligned, lock-free MPMC ring buffer. Every slot occupies
// exactly one 64-byte cache line so producers and consumers never
// false-share. Power-of-two capacity; Vyukov MPMC queue discipline
// adapted for fixed-size slots.
//
// Zero allocation on Push/Pop/BatchDrain in steady state. No locks. The
// only allocation is the backing slices in New.
//
// Slot fields are deliberately generic: Seq and Timestamp are owned by
// the ring, everything else is caller-defined. If a caller needs more
// than 8 bytes of payload, store a handle (index, hash, offset) in
// Payload and keep the bulk data in a side buffer the caller owns. A
// fixed-size slot is the entire point; variable-size payloads defeat it.
package ringbuf

import (
	"sync/atomic"
	"unsafe"
)

// Slot is exactly one cache line (64 bytes on all supported
// architectures). Size is enforced by a compile-time assertion below.
type Slot struct {
	Seq       uint64   // offset  0 — written by the ring
	Timestamp int64    // offset  8 — caller-defined
	A         int64    // offset 16 — caller-defined
	B         int64    // offset 24 — caller-defined
	ID        [16]byte // offset 32 — caller-defined binary identifier
	Status    uint16   // offset 48 — caller-defined
	Kind      uint8    // offset 50 — caller-defined
	_         [5]byte  // offset 51 — pad to 8-byte alignment
	Payload   [8]byte  // offset 56 — caller-defined short payload
}

const (
	slotSize        = 64
	defaultCapacity = 1024
	maxCapacity     = 1 << 30 // sane upper bound; guards New against overflow
)

// Compile-time assertion: Slot must be exactly slotSize bytes. The
// paired negative-index trick fails the build in either direction.
var (
	_ [slotSize - int(unsafe.Sizeof(Slot{}))]struct{}
	_ [int(unsafe.Sizeof(Slot{})) - slotSize]struct{}
)

// paddedSeq keeps each sequence counter on its own cache line, so a
// producer touching slot i's sequence does not evict slot i+1's
// sequence from a neighbouring core.
type paddedSeq struct {
	val atomic.Uint64
	_   [56]byte
}

var (
	_ [slotSize - int(unsafe.Sizeof(paddedSeq{}))]struct{}
	_ [int(unsafe.Sizeof(paddedSeq{})) - slotSize]struct{}
)

// Ring is a fixed-capacity MPMC ring buffer. Use New.
//
// head and tail are padded to cache lines so concurrent producers and
// consumers do not false-share the two counters.
type Ring struct {
	_        [56]byte
	head     atomic.Uint64
	_        [56]byte
	tail     atomic.Uint64
	_        [56]byte
	slots    []Slot
	sequence []paddedSeq
	mask     uint64
}

// New allocates a Ring. A capacity below 2 is replaced with
// defaultCapacity (1024); otherwise capacity is rounded up to the next
// power of two and clamped to maxCapacity. The smallest ring
// obtainable is New(2).
func New(capacity int) *Ring {
	if capacity < 2 {
		capacity = defaultCapacity
	}
	if capacity > maxCapacity {
		capacity = maxCapacity
	}
	cap2 := 2
	for cap2 < capacity {
		cap2 <<= 1
	}
	r := &Ring{
		slots:    make([]Slot, cap2),
		sequence: make([]paddedSeq, cap2),
		mask:     uint64(cap2 - 1), //nolint:gosec // cap2 >= 2 and <= 2^30, fits in uint64
	}
	for i := range r.sequence {
		r.sequence[i].val.Store(uint64(i))
	}
	return r
}

// Push enqueues one slot. Returns false when the buffer is full.
// The caller's Slot is copied by value; the caller may reuse it
// immediately.
func (r *Ring) Push(s *Slot) bool {
	for {
		head := r.head.Load()
		seq := r.sequence[head&r.mask].val.Load()
		diff := int64(seq) - int64(head) //nolint:gosec // intentional 2's-complement wraparound
		switch {
		case diff == 0:
			if r.head.CompareAndSwap(head, head+1) {
				slot := &r.slots[head&r.mask]
				*slot = *s
				slot.Seq = head
				r.sequence[head&r.mask].val.Store(head + 1)
				return true
			}
		case diff < 0:
			return false // full
		default:
			// another producer advanced head; re-read and retry
		}
	}
}

// Pop dequeues one slot into dst. Returns false when the buffer is
// empty.
func (r *Ring) Pop(dst *Slot) bool {
	for {
		tail := r.tail.Load()
		next := tail + 1
		seq := r.sequence[tail&r.mask].val.Load()
		diff := int64(seq) - int64(next) //nolint:gosec // intentional 2's-complement wraparound
		switch {
		case diff == 0:
			if r.tail.CompareAndSwap(tail, next) {
				*dst = r.slots[tail&r.mask]
				r.sequence[tail&r.mask].val.Store(tail + r.mask + 1)
				return true
			}
		case diff < 0:
			return false // empty
		default:
			// another consumer advanced tail; re-read and retry
		}
	}
}

// BatchDrain pops up to len(dst) slots. Returns the number drained.
func (r *Ring) BatchDrain(dst []Slot) int {
	n := 0
	for n < len(dst) {
		if !r.Pop(&dst[n]) {
			break
		}
		n++
	}
	return n
}

// Len returns a best-effort snapshot of queued slots; under concurrent
// Push/Pop it may be stale by the time it returns. Do not use it for
// correctness-critical decisions.
func (r *Ring) Len() int {
	h := r.head.Load()
	t := r.tail.Load()
	if h >= t {
		return int(h - t) //nolint:gosec // h>=t checked above; diff bounded by capacity
	}
	return 0
}

// Cap returns the ring's fixed capacity (a power of two, >= 2).
func (r *Ring) Cap() int {
	return int(r.mask) + 1 //nolint:gosec // mask <= 2^30-1
}
