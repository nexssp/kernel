package ringbuf_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/nexssp/kernel/ringbuf"
)

func TestSlotIsExactlyCacheLine(t *testing.T) {
	if got := unsafe.Sizeof(ringbuf.Slot{}); got != 64 {
		t.Fatalf("Slot size = %d, want 64", got)
	}
}

func TestRingPushPop(t *testing.T) {
	r := ringbuf.New(8)
	var in ringbuf.Slot
	in.Status = 200
	in.Kind = 7
	in.A = 42
	copy(in.Payload[:], "hello")
	if !r.Push(&in) {
		t.Fatal("push failed")
	}
	var out ringbuf.Slot
	if !r.Pop(&out) {
		t.Fatal("pop failed")
	}
	if out.Status != 200 || out.Kind != 7 || out.A != 42 {
		t.Fatalf("slot corrupted: %+v", out)
	}
	if string(out.Payload[:5]) != "hello" {
		t.Fatalf("payload corrupted: %q", string(out.Payload[:]))
	}
}

func TestRingEmptyPopReturnsFalse(t *testing.T) {
	r := ringbuf.New(8)
	var out ringbuf.Slot
	if r.Pop(&out) {
		t.Fatal("pop on empty ring succeeded")
	}
}

func TestRingFullRejects(t *testing.T) {
	r := ringbuf.New(4)
	var s ringbuf.Slot
	for i := range 4 {
		if !r.Push(&s) {
			t.Fatalf("push %d rejected", i)
		}
	}
	if r.Push(&s) {
		t.Fatal("push on full ring succeeded")
	}
}

func TestRingOrderIsFIFO(t *testing.T) {
	r := ringbuf.New(8)
	for i := range uint64(8) {
		var s ringbuf.Slot
		s.A = int64(i)
		if !r.Push(&s) {
			t.Fatalf("push %d rejected", i)
		}
	}
	for i := range int64(8) {
		var s ringbuf.Slot
		if !r.Pop(&s) {
			t.Fatalf("pop %d failed", i)
		}
		if s.A != i {
			t.Fatalf("out of order: got %d, want %d", s.A, i)
		}
	}
}

func TestRingWraparoundPreservesData(t *testing.T) {
	r := ringbuf.New(4)
	var s ringbuf.Slot
	// push/pop 4× capacity to force wrap
	for i := range int64(16) {
		s.A = i
		if !r.Push(&s) {
			t.Fatalf("push %d rejected", i)
		}
		var out ringbuf.Slot
		if !r.Pop(&out) {
			t.Fatalf("pop %d failed", i)
		}
		if out.A != i {
			t.Fatalf("wrap corrupted: got %d, want %d", out.A, i)
		}
	}
}

func TestRingNewClampsCapacity(t *testing.T) {
	for _, in := range []int{0, 1, -5} {
		r := ringbuf.New(in)
		if r.Cap() != 1024 {
			t.Fatalf("New(%d).Cap() = %d, want 1024", in, r.Cap())
		}
	}
}

func TestRingNewRoundsToPowerOfTwo(t *testing.T) {
	cases := map[int]int{2: 2, 3: 4, 5: 8, 17: 32, 100: 128, 1024: 1024, 1025: 2048}
	for in, want := range cases {
		if got := ringbuf.New(in).Cap(); got != want {
			t.Errorf("New(%d).Cap() = %d, want %d", in, got, want)
		}
	}
}

func TestRingLen(t *testing.T) {
	r := ringbuf.New(8)
	if r.Len() != 0 {
		t.Fatalf("empty Len = %d", r.Len())
	}
	var s ringbuf.Slot
	r.Push(&s)
	r.Push(&s)
	r.Push(&s)
	if r.Len() != 3 {
		t.Fatalf("Len = %d, want 3", r.Len())
	}
	var out ringbuf.Slot
	r.Pop(&out)
	if r.Len() != 2 {
		t.Fatalf("Len after pop = %d, want 2", r.Len())
	}
}

func TestBatchDrain_StopsAtEmpty(t *testing.T) {
	r := ringbuf.New(8)
	var s ringbuf.Slot
	for i := range 3 {
		s.A = int64(i)
		r.Push(&s)
	}
	buf := make([]ringbuf.Slot, 8)
	n := r.BatchDrain(buf)
	if n != 3 {
		t.Fatalf("drained %d, want 3", n)
	}
	for i := range 3 {
		if buf[i].A != int64(i) {
			t.Fatalf("buf[%d].A = %d, want %d", i, buf[i].A, i)
		}
	}
}

func TestBatchDrain_HonoursDstLen(t *testing.T) {
	r := ringbuf.New(16)
	var s ringbuf.Slot
	for range 10 {
		r.Push(&s)
	}
	buf := make([]ringbuf.Slot, 4)
	n := r.BatchDrain(buf)
	if n != 4 {
		t.Fatalf("drained %d, want 4 (dst len)", n)
	}
	if r.Len() != 6 {
		t.Fatalf("ring Len = %d, want 6", r.Len())
	}
}

func TestBatchDrain_ZeroLenDstIsNoop(t *testing.T) {
	r := ringbuf.New(8)
	var s ringbuf.Slot
	r.Push(&s)
	if n := r.BatchDrain(nil); n != 0 {
		t.Fatalf("drained %d from nil dst", n)
	}
	if r.Len() != 1 {
		t.Fatal("ring lost an item")
	}
}

func TestRingPushZeroAlloc(t *testing.T) {
	r := ringbuf.New(1024)
	var s ringbuf.Slot
	allocs := testing.AllocsPerRun(10_000, func() {
		if !r.Push(&s) {
			panic("push rejected")
		}
		var out ringbuf.Slot
		if !r.Pop(&out) {
			panic("pop rejected")
		}
	})
	if allocs != 0 {
		t.Fatalf("push+pop steady state allocated %.2f per call", allocs)
	}
}

func TestRingBatchDrainZeroAlloc(t *testing.T) {
	r := ringbuf.New(1024)
	var buf [256]ringbuf.Slot
	var s ringbuf.Slot
	// Pre-fill so drain always has work.
	for range 512 {
		r.Push(&s)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		n := r.BatchDrain(buf[:])
		if n == 0 {
			panic("expected work")
		}
		// refill so next iteration also has work
		for range n {
			r.Push(&s)
		}
	})
	if allocs != 0 {
		t.Fatalf("BatchDrain allocated %.2f per call", allocs)
	}
}

func TestRingLenCapZeroAlloc(t *testing.T) {
	r := ringbuf.New(1024)
	allocs := testing.AllocsPerRun(10_000, func() {
		_ = r.Len()
		_ = r.Cap()
	})
	if allocs != 0 {
		t.Fatalf("Len/Cap allocated %.2f per call", allocs)
	}
}

func TestRingConcurrentMPMC(t *testing.T) {
	const (
		producers       = 8
		consumers       = 4
		perProducer     = 5000
		total           = producers * perProducer
		ringCapacity    = 512
		consumerBackoff = 50 * time.Microsecond
	)

	r := ringbuf.New(ringCapacity)

	var (
		pushed atomic.Int64
		popped atomic.Int64
		pWG    sync.WaitGroup
		cWG    sync.WaitGroup
		stop   = make(chan struct{})
	)

	for range producers {
		pWG.Go(func() {
			var s ringbuf.Slot
			for range perProducer {
				for !r.Push(&s) {
					runtime.Gosched()
				}
				pushed.Add(1)
			}
		})
	}

	for range consumers {
		cWG.Go(func() {
			var s ringbuf.Slot
			for {
				if r.Pop(&s) {
					popped.Add(1)
					continue
				}
				if pushed.Load() == total {
					// producers done; drain remaining and exit
					for r.Pop(&s) {
						popped.Add(1)
					}
					return
				}
				select {
				case <-stop:
					for r.Pop(&s) {
						popped.Add(1)
					}
					return
				case <-time.After(consumerBackoff):
				}
			}
		})
	}

	pWG.Wait()
	close(stop)
	cWG.Wait()

	if got := popped.Load(); got != total {
		t.Fatalf("popped=%d, want %d", got, total)
	}
	if r.Len() != 0 {
		t.Fatalf("ring not empty after drain: Len=%d", r.Len())
	}
}

//
// Run with: go test -bench=. -benchmem -cpu=1,4,8 ./kernel/ringbuf/
//
// Steady state expectations:
//
//	Push+Pop     0 allocs/op, 0 B/op
//	BatchDrain   0 allocs/op, 0 B/op
//
// Throughput will scale sub-linearly past NumCPU because head and tail
// contend on the same cache lines (that is the MPMC trade-off;
// BatchDrain reduces the contention per item).

func BenchmarkRingPushPop(b *testing.B) {
	r := ringbuf.New(1024)
	var s ringbuf.Slot
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Push(&s)
		r.Pop(&s)
	}
}

func BenchmarkRingBatchDrain(b *testing.B) {
	r := ringbuf.New(1024)
	var s ringbuf.Slot
	var buf [64]ringbuf.Slot
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.Push(&s)
		r.BatchDrain(buf[:])
	}
}

func BenchmarkRingPushPopParallel(b *testing.B) {
	r := ringbuf.New(1024)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var s, out ringbuf.Slot
		for pb.Next() {
			r.Push(&s)
			r.Pop(&out)
		}
	})
}
