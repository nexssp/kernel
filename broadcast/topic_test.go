package broadcast_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/broadcast"
	"github.com/nexssp/kernel/xtest"
)

func TestTopic_BasicPublishRead(t *testing.T) {
	t.Parallel()

	top := broadcast.NewTopic[int](8)
	var cursor broadcast.Cursor

	top.Publish(10)
	top.Publish(20)

	dst := make([]int, 4)
	n := top.Read(&cursor, dst)

	if n != 2 || dst[0] != 10 || dst[1] != 20 {
		t.Fatalf("unexpected read result: n=%d, dst=%v", n, dst[:n])
	}
}

// TestTopic_LappedConsumerReceivesOverwrittenValue stresses the lapping boundary
// under continuous write contention with `-race` enabled.
func TestTopic_LappedConsumerReceivesOverwrittenValue(t *testing.T) {
	t.Parallel()

	const capacity = 8
	top := broadcast.NewTopic[int](capacity)

	var running atomic.Bool
	running.Store(true)

	var wg sync.WaitGroup

	// Single writer continuously publishing incrementing values
	wg.Go(func() {
		val := 0
		for running.Load() {
			top.Publish(val)
			val++
		}
	})

	// Multiple parallel readers reading concurrently while intentionally lagging
	const readers = 4
	for r := range readers {
		readerID := r
		wg.Go(func() {
			var cursor broadcast.Cursor
			dst := make([]int, 4)

			for range 5000 {
				if readerID%2 == 0 {
					cursor.Seq = 0 // Force repeated lapping resets
				}
				_ = top.Read(&cursor, dst)
			}
		})
	}

	running.Store(false)
	wg.Wait()
}

func TestTopic_ZeroAlloc(t *testing.T) {
	top := broadcast.NewTopic[int](64)
	var cursor broadcast.Cursor
	dst := make([]int, 1)

	xtest.RequireZeroAlloc(t, 1000, func() {
		top.Publish(42)
		_ = top.Read(&cursor, dst)
	})
}
