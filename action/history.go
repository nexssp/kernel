package action

import (
	"context"
	"sync"
	"time"
)

type Record[Req, Res any] struct {
	Time     time.Time
	Duration time.Duration
	Req      Req
	Res      Res
	Err      error
}

type History[Req, Res any] struct {
	mu   sync.Mutex
	buf  []Record[Req, Res]
	pos  int
	full bool
}

func NewHistory[Req, Res any](capacity int) *History[Req, Res] {
	if capacity < 1 {
		capacity = 1 // Prevent divide-by-zero in Push and allocate minimal ring
	}
	return &History[Req, Res]{
		buf: make([]Record[Req, Res], capacity),
	}
}

func (h *History[Req, Res]) Push(rec Record[Req, Res]) {
	h.mu.Lock()
	h.buf[h.pos] = rec
	h.pos = (h.pos + 1) % len(h.buf)
	if h.pos == 0 {
		h.full = true
	}
	h.mu.Unlock()
}

func (h *History[Req, Res]) Snapshot() []Record[Req, Res] {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.full {
		out := make([]Record[Req, Res], h.pos)
		copy(out, h.buf[:h.pos])
		return out
	}
	out := make([]Record[Req, Res], len(h.buf))
	copy(out, append(h.buf[h.pos:], h.buf[:h.pos]...))
	return out
}

func HistoryMiddleware[Req, Res any](hist *History[Req, Res]) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (Res, error) {
			start := time.Now()
			res, err := next(ctx, req)
			hist.Push(Record[Req, Res]{
				Time:     start,
				Duration: time.Since(start),
				Req:      req,
				Res:      res,
				Err:      err,
			})
			return res, err
		}
	}
}
