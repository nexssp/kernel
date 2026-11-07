// Package timing provides a demand-driven Hashed Timing Wheel for tracking
// millions of short-lived timeouts without allocating OS timers.
//
// Concurrency model: Wheel is not thread-safe and must be driven from a
// single goroutine (e.g., the main game loop).
package timing

import (
	"errors"
	"math"
	"slices"
	"time"
)

const maxSimpleAdvanceTicks = 64

// ErrNegativeAdvance is returned when AdvanceChecked receives a negative delta.
var ErrNegativeAdvance = errors.New("timing wheel advance duration must not be negative")

// TimerID uniquely identifies a scheduled timer for O(1) cancellation.
// Upper 32 bits = epoch (generation), Lower 32 bits = slot index.
type TimerID uint64

type timerNode struct {
	id        TimerID
	payload   any
	rotations int64
	prev      int
	next      int
	canceled  bool
}

type wheelEvent struct {
	tick  int64
	index int
	slot  int
	order int
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
	events     []wheelEvent
}

// New creates a timing wheel with the given resolution (tick size).
// nodeCapacity pre-allocates the fixed-size node pool. It returns nil for
// invalid dimensions; use NewChecked when the caller needs the error.
func New(resolution time.Duration, slots, nodeCapacity int) *Wheel {
	wheel, err := NewChecked(resolution, slots, nodeCapacity)
	if err != nil {
		return nil
	}
	return wheel
}

// NewChecked creates a timing wheel and reports invalid dimensions.
func NewChecked(resolution time.Duration, slots, nodeCapacity int) (*Wheel, error) {
	if resolution <= 0 {
		return nil, errors.New("timing wheel resolution must be positive")
	}
	if slots <= 0 {
		return nil, errors.New("timing wheel slot count must be positive")
	}
	if nodeCapacity <= 0 {
		return nil, errors.New("timing wheel node capacity must be positive")
	}

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
	return w, nil
}

// Schedule adds a timeout. Returns an error if the node pool is exhausted.
func (w *Wheel) Schedule(delay time.Duration, payload any) (TimerID, error) {
	if w.freeHead == -1 {
		return 0, errors.New("timing wheel node pool exhausted")
	}

	// The deadline is measured from the current point within a tick, so a
	// fractional tick must be rounded up rather than truncated. Decompose the
	// duration to avoid overflowing when adding the remainder.
	ticks := delay / w.resolution
	delayRemainder := delay % w.resolution
	if delayRemainder != 0 || w.remainder != 0 {
		ticks++
		if delayRemainder > w.resolution-w.remainder {
			ticks++
		}
	}
	ticks = max(ticks, 1)
	slotCount := len(w.slots)
	slotCountDuration := time.Duration(slotCount)
	current := time.Duration(w.current)
	slotOffset := ticks % slotCountDuration
	var slot time.Duration
	if slotOffset < slotCountDuration-current {
		slot = current + slotOffset
	} else {
		slot = slotOffset - (slotCountDuration - current)
	}

	// Subtract 1 before division so exact cycle multiples (e.g. ticks == len(slots))
	// evaluate to 0 rotations instead of 1, firing on the first full rotation.
	rotations := int64((ticks - 1) / slotCountDuration)

	nodeIdx := w.freeHead
	w.freeHead = w.nodes[nodeIdx].next

	w.epoch++
	if w.epoch == 0 {
		w.epoch = 1
	}
	//nolint:gosec // nodeIdx is positive index in nodes array
	id := (TimerID(w.epoch) << 32) | TimerID(nodeIdx)

	next := w.slots[slot]
	w.nodes[nodeIdx] = timerNode{
		id:        id,
		payload:   payload,
		rotations: rotations,
		prev:      -1,
		next:      next,
		canceled:  false,
	}
	if next != -1 {
		w.nodes[next].prev = nodeIdx
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
// For a negative delta it leaves the wheel unchanged; use AdvanceChecked when
// the caller needs to observe the validation error.
func (w *Wheel) Advance(dt time.Duration, dst []any) []any {
	advanced, err := w.AdvanceChecked(dt, dst)
	if err != nil {
		return dst
	}
	return advanced
}

// AdvanceChecked advances the wheel and reports invalid negative deltas.
// Large catch-up intervals are processed in work bounded by the node pool,
// preserving timer firing order and cancellation cleanup semantics.
func (w *Wheel) AdvanceChecked(dt time.Duration, dst []any) ([]any, error) {
	if dt < 0 {
		return dst, ErrNegativeAdvance
	}
	ticks, remainder := advanceTicks(dt, w.remainder, w.resolution)
	w.remainder = remainder
	if ticks == 0 {
		return dst, nil
	}
	if ticks <= maxSimpleAdvanceTicks {
		return w.advanceSimple(int(ticks), dst), nil
	}
	return w.advanceMany(ticks, dst), nil
}

func advanceTicks(dt, remainder, resolution time.Duration) (int64, time.Duration) {
	ticks := int64(dt / resolution)
	partial := dt % resolution
	remaining := resolution - remainder
	if partial >= remaining {
		ticks++
		return ticks, partial - remaining
	}
	return ticks, remainder + partial
}

func (w *Wheel) advanceSimple(ticks int, dst []any) []any {
	for range ticks {
		w.current = (w.current + 1) % len(w.slots)

		curr := w.slots[w.current]

		for curr != -1 {
			node := &w.nodes[curr]
			next := node.next

			if node.canceled {
				w.unlinkAndFree(w.current, curr)
				curr = next
				continue
			}

			if node.rotations > 0 {
				node.rotations--
				curr = next
				continue
			}

			dst = append(dst, node.payload)
			w.unlinkAndFree(w.current, curr)
			curr = next
		}
	}
	return dst
}

func (w *Wheel) advanceMany(ticks int64, dst []any) []any {
	cycle := int64(len(w.slots))
	events := w.events[:0]
	for slot, head := range w.slots {
		firstVisit := int64(slot - w.current)
		if firstVisit <= 0 {
			firstVisit += cycle
		}
		var visits int64
		if ticks >= firstVisit {
			visits = 1 + (ticks-firstVisit)/cycle
		}
		order := 0
		for index := head; index != -1; index = w.nodes[index].next {
			node := &w.nodes[index]
			switch {
			case node.canceled && visits > 0:
				events = append(events, wheelEvent{tick: firstVisit, index: index, slot: slot, order: order})
			case !node.canceled && visits > node.rotations:
				fireAt := firstVisit + node.rotations*cycle
				events = append(events, wheelEvent{tick: fireAt, index: index, slot: slot, order: order})
			case !node.canceled:
				node.rotations -= visits
			}
			order++
		}
	}

	slices.SortFunc(events, compareWheelEvents)
	for _, event := range events {
		node := &w.nodes[event.index]
		if !node.canceled {
			dst = append(dst, node.payload)
		}
		w.unlinkAndFree(event.slot, event.index)
	}
	w.events = events[:0]
	w.advanceCurrent(ticks)
	return dst
}

func compareWheelEvents(a, b wheelEvent) int {
	if a.tick < b.tick {
		return -1
	}
	if a.tick > b.tick {
		return 1
	}
	if a.slot < b.slot {
		return -1
	}
	if a.slot > b.slot {
		return 1
	}
	if a.order < b.order {
		return -1
	}
	if a.order > b.order {
		return 1
	}
	return 0
}

func (w *Wheel) advanceCurrent(ticks int64) {
	cycle := int64(len(w.slots))
	offset := int(ticks % cycle)
	untilWrap := len(w.slots) - w.current
	if offset >= untilWrap {
		w.current = offset - untilWrap
		return
	}
	w.current += offset
}

func (w *Wheel) unlinkAndFree(slot, current int) {
	node := &w.nodes[current]
	if node.prev == -1 {
		w.slots[slot] = node.next
	} else {
		w.nodes[node.prev].next = node.next
	}
	if node.next != -1 {
		w.nodes[node.next].prev = node.prev
	}
	node.prev = -1
	node.next = w.freeHead
	w.freeHead = current
}
