package ringbuf_test

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/nexssp/kernel/ringbuf"
)

type bufferBudgetItem [1024]byte

func TestNewBufferMemoryBudget(t *testing.T) {
	t.Run("default rejects before allocation", func(t *testing.T) {
		capacity := int(ringbuf.DefaultMemoryBudgetBytes / uint64(unsafe.Sizeof(bufferBudgetItem{})))
		if _, err := ringbuf.NewBufferChecked[bufferBudgetItem](capacity); !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
			t.Fatalf("NewBufferChecked() error = %v, want ErrMemoryBudgetExceeded", err)
		}
		if got := ringbuf.NewBuffer[bufferBudgetItem](capacity); got != nil {
			t.Fatal("NewBuffer() allocated a buffer beyond its default budget")
		}
	})

	t.Run("explicit per-buffer budget", func(t *testing.T) {
		buffer, err := ringbuf.NewBufferWithOptions[bufferBudgetItem](ringbuf.BufferOptions{
			Capacity:          2,
			MemoryBudgetBytes: 4096,
		})
		if err != nil {
			t.Fatalf("NewBufferWithOptions() error = %v", err)
		}
		if got := len(buffer.Snapshot()); got != 0 {
			t.Fatalf("empty snapshot length = %d, want 0", got)
		}

		_, err = ringbuf.NewBufferWithOptions[bufferBudgetItem](ringbuf.BufferOptions{
			Capacity:          2,
			MemoryBudgetBytes: 1024,
		})
		if !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
			t.Fatalf("undersized explicit budget error = %v, want ErrMemoryBudgetExceeded", err)
		}
	})

	t.Run("zero budget rejects", func(t *testing.T) {
		_, err := ringbuf.NewBufferWithOptions[int](ringbuf.BufferOptions{Capacity: 1})
		if !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
			t.Fatalf("zero budget error = %v, want ErrMemoryBudgetExceeded", err)
		}
	})

	t.Run("rejection does not allocate backing storage", func(t *testing.T) {
		capacity := int(ringbuf.DefaultMemoryBudgetBytes / uint64(unsafe.Sizeof(bufferBudgetItem{})))
		allocs := testing.AllocsPerRun(100, func() {
			if _, err := ringbuf.NewBufferChecked[bufferBudgetItem](capacity); !errors.Is(err, ringbuf.ErrMemoryBudgetExceeded) {
				t.Fatalf("NewBufferChecked() error = %v, want ErrMemoryBudgetExceeded", err)
			}
		})
		if allocs != 0 {
			t.Fatalf("rejected construction allocated %.2f objects per call", allocs)
		}
	})
}

func TestNewBufferNormalizesNonPositiveCapacity(t *testing.T) {
	buffer, err := ringbuf.NewBufferChecked[int](0)
	if err != nil {
		t.Fatalf("NewBufferChecked(0) error = %v", err)
	}
	buffer.Push(7)
	if got := buffer.Snapshot(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("normalized buffer snapshot = %v, want [7]", got)
	}
}
