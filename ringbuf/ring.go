// package ringbuf
// Cache-line-aligned, lock-free MPMC ring buffer. Every slot occupies
// exactly one 64-byte cache line so producers and consumers never
// false-share. Power-of-two capacity; Vyukov MPMC queue discipline
// adapted for fixed-size slots.
//
// Zero allocation on Push/Pop/BatchDrain in steady state. No locks. New
// allocates only after checking the Ring header and backing slices against
// the configured memory budget.
//
// Slot fields are deliberately generic: Seq and Timestamp are owned by
// the ring, everything else is caller-defined. If a caller needs more
// than 8 bytes of payload, store a handle (index, hash, offset) in
// Payload and keep the bulk data in a side buffer the caller owns. A
// fixed-size slot is the entire point; variable-size payloads defeat it.
package ringbuf

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/nexssp/kernel/internal/capacity"
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
	maxCapacity     = capacity.MaxPowerOfTwo // hard capacity bound, independent of the memory budget
	lenMaxAttempts  = 8                      // Eight attempts tolerate brief contention while bounding Len latency.
)

// DefaultMemoryBudgetBytes is the default per-Ring memory budget (64 MiB).
const DefaultMemoryBudgetBytes uint64 = 64 << 20

var (
	// ErrInvalidCapacity reports a capacity beyond the supported maximum.
	ErrInvalidCapacity = errors.New("ringbuf: invalid capacity")
	// ErrMemoryBudgetExceeded reports a ring allocation larger than its budget.
	ErrMemoryBudgetExceeded = errors.New("ringbuf: memory budget exceeded")
)

// Options configure one Ring allocation. Capacity below 2 retains New's
// historical default-capacity behavior. MemoryBudgetBytes is required and
// includes the Ring header plus its backing arrays, excluding allocator
// metadata and size-class rounding.
type Options struct {
	Capacity          int
	MemoryBudgetBytes uint64
}

// Compile-time assertion: Slot must be exactly slotSize bytes. The
// paired negative-index trick fails the build in either direction.
var (
	_ [slotSize - int(unsafe.Sizeof(Slot{}))]struct{}
	_ [int(unsafe.Sizeof(Slot{})) - slotSize]struct{}
)

// paddedSeq keeps each sequence counter on its own cache line, so a
// producer touching slot i's sequence does not evict slot i+1's
// sequence from a neighboring core.
type paddedSeq struct {
	val atomic.Uint64
	_   [56]byte
}

var (
	_ [slotSize - int(unsafe.Sizeof(paddedSeq{}))]struct{}
	_ [int(unsafe.Sizeof(paddedSeq{})) - slotSize]struct{}
)

// Ring is a fixed-capacity MPMC ring buffer. Use New or NewWithOptions.
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

// New allocates a Ring using the default 64 MiB budget. A capacity below 2
// is replaced with defaultCapacity (1024); otherwise capacity is rounded up
// to the next power of two. It returns nil when validation fails; use
// NewChecked to inspect the error. Requests are never clamped to a smaller
// capacity.
func New(requestedCapacity int) *Ring {
	ring, err := NewChecked(requestedCapacity)
	if err != nil {
		return nil
	}
	return ring
}

// NewChecked allocates a Ring using DefaultMemoryBudgetBytes and reports
// invalid or over-budget capacities before allocating.
func NewChecked(requestedCapacity int) (*Ring, error) {
	return NewWithOptions(Options{
		Capacity:          requestedCapacity,
		MemoryBudgetBytes: DefaultMemoryBudgetBytes,
	})
}

// NewWithOptions allocates a Ring after validating its rounded capacity and
// estimated allocation size. MemoryBudgetBytes must be nonzero; the caller
// may explicitly choose a budget different from DefaultMemoryBudgetBytes.
func NewWithOptions(options Options) (*Ring, error) {
	requestedCapacity := options.Capacity
	if requestedCapacity < 2 {
		requestedCapacity = defaultCapacity
	}
	cap2, ok := capacity.PowerOfTwoCeiling(requestedCapacity, maxCapacity)
	if !ok {
		return nil, fmt.Errorf("ringbuf: capacity %d exceeds maximum %d: %w", requestedCapacity, maxCapacity, ErrInvalidCapacity)
	}
	requiredBytes := ringAllocationBytes(cap2)
	if requiredBytes > options.MemoryBudgetBytes {
		return nil, fmt.Errorf("ringbuf: capacity %d requires %d bytes, budget is %d: %w", cap2, requiredBytes, options.MemoryBudgetBytes, ErrMemoryBudgetExceeded)
	}
	r := &Ring{
		slots:    make([]Slot, cap2),
		sequence: make([]paddedSeq, cap2),
		mask:     uint64(cap2 - 1), //nolint:gosec // cap2 >= 2 and <= 2^30, fits in uint64
	}
	for i := range r.sequence {
		r.sequence[i].val.Store(uint64(i))
	}
	return r, nil
}

func ringAllocationBytes(capacitySlots int) uint64 {
	bytesPerSlot := uint64(unsafe.Sizeof(Slot{})) + uint64(unsafe.Sizeof(paddedSeq{}))
	//nolint:gosec // capacitySlots is normalized to [2, maxCapacity] before use.
	slots := uint64(capacitySlots)
	return slots*bytesPerSlot + uint64(unsafe.Sizeof(Ring{}))
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
// Push/Pop it may be stale by the time it returns. If no stable snapshot is
// found within eight attempts, it returns 0. Do not use it for
// correctness-critical decisions.
func (r *Ring) Len() int {
	for range lenMaxAttempts {
		head := r.head.Load()
		tail := r.tail.Load()
		if r.head.Load() != head {
			continue
		}
		// With head stable across the tail read, tail cannot legitimately be
		// ahead of it. Unsigned subtraction also handles counter wraparound.
		return int(head - tail) //nolint:gosec // distance is bounded by ring capacity
	}
	return 0
}

// Cap returns the ring's fixed capacity (a power of two, >= 2).
func (r *Ring) Cap() int {
	return int(r.mask) + 1 //nolint:gosec // mask <= 2^30-1
}
