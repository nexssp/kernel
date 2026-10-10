// Package timing provides a demand-driven Hashed Timing Wheel for tracking
// millions of short-lived timeouts without allocating OS timers.
//
// Concurrency model: Wheel is not thread-safe and must be driven from a
// single goroutine (e.g., the main game loop).
package timing

import (
	"errors"
	"math"
	"time"
)

// TimerID uniquely identifies a scheduled timer for O(1) cancellation.
// Upper 32 bits = epoch (generation), Lower 32 bits = slot index.
type TimerID uint64

type timerNode struct {
	id        TimerID
	payload   any
	rotations int
	next      int
	canceled  bool
}

// Wheel is an O(1) hashed timing wheel. It is demand-driven: the caller
// pumps it by calling Advance(dt).
type Wheel struct {
	resolution time.Duration
	slots      []int
	nodes      []timerNode
	freeHead   int
	current    int
	remainder  time.Duration
	epoch      uint32
}

// New creates a timing wheel with the given resolution (tick size).
// nodeCapacity pre-allocates the fixed-size node pool.
func New(resolution time.Duration, slots, nodeCapacity int) *Wheel {
	w := &Wheel{
		resolution: resolution,
		slots:      make([]int, slots),
		nodes:      make([]timerNode, nodeCapacity),
		freeHead:   0,
		epoch:      1,
	}
	for i := range w.slots {
		w.slots[i] = -1
	}
	for i := range w.nodes {
		w.nodes[i].next = i + 1
	}
	w.nodes[nodeCapacity-1].next = -1
	return w
}

// Schedule adds a timeout. Returns an error if the node pool is exhausted.
func (w *Wheel) Schedule(delay time.Duration, payload any) (TimerID, error) {
	if w.freeHead == -1 {
		return 0, errors.New("timing wheel node pool exhausted")
	}

	ticks := max(int((delay+w.remainder)/w.resolution), 1)
	slot := (w.current + ticks) % len(w.slots)

	// Subtract 1 before division so exact cycle multiples (e.g. ticks == len(slots))
	// evaluate to 0 rotations instead of 1, firing on the first full rotation.
	rotations := (ticks - 1) / len(w.slots)

	nodeIdx := w.freeHead
	w.freeHead = w.nodes[nodeIdx].next

	w.epoch++
	if w.epoch == 0 {
		w.epoch = 1
	}
	//nolint:gosec // nodeIdx is positive index in nodes array
	id := (TimerID(w.epoch) << 32) | TimerID(nodeIdx)

	w.nodes[nodeIdx] = timerNode{
		id:        id,
		payload:   payload,
		rotations: rotations,
		next:      w.slots[slot],
		canceled:  false,
	}
	w.slots[slot] = nodeIdx

	return id, nil
}

// Cancel removes a scheduled timer in O(1) time without allocations.
func (w *Wheel) Cancel(id TimerID) bool {
	idx := int(id & math.MaxUint32)
	if idx < 0 || idx >= len(w.nodes) {
		return false
	}
	if w.nodes[idx].id == id && !w.nodes[idx].canceled {
		w.nodes[idx].canceled = true
		return true
	}
	return false
}

// Advance moves the wheel forward by dt and appends expired payloads to dst.
func (w *Wheel) Advance(dt time.Duration, dst []any) []any {
	w.remainder += dt
	ticks := int(w.remainder / w.resolution)
	w.remainder -= time.Duration(ticks) * w.resolution

	for range ticks {
		w.current = (w.current + 1) % len(w.slots)

		prev := -1
		curr := w.slots[w.current]

		for curr != -1 {
			node := &w.nodes[curr]
			next := node.next

			if node.canceled {
				w.unlinkAndFree(prev, curr, next)
				curr = next
				continue
			}

			if node.rotations > 0 {
				node.rotations--
				prev = curr
				curr = next
				continue
			}

			dst = append(dst, node.payload)
			w.unlinkAndFree(prev, curr, next)
			curr = next
		}
	}
	return dst
}

func (w *Wheel) unlinkAndFree(prev, curr, next int) {
	if prev == -1 {
		w.slots[w.current] = next
	} else {
		w.nodes[prev].next = next
	}
	w.nodes[curr].next = w.freeHead
	w.freeHead = curr
}
