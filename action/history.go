package action

import (
	"context"
	"time"

	"github.com/nexssp/kernel/ringbuf"
)

type Record[Req, Res any] struct {
	Time     time.Time
	Duration time.Duration
	Req      Req
	Res      Res
	Err      error
}

type History[Req, Res any] struct {
	buf *ringbuf.Buffer[Record[Req, Res]]
}

func NewHistory[Req, Res any](capacity int) *History[Req, Res] {
	return &History[Req, Res]{
		buf: ringbuf.NewBuffer[Record[Req, Res]](capacity),
	}
}

func (h *History[Req, Res]) Push(rec Record[Req, Res]) {
	if h == nil || h.buf == nil {
		return
	}
	h.buf.Push(rec)
}

func (h *History[Req, Res]) Snapshot() []Record[Req, Res] {
	if h == nil || h.buf == nil {
		return nil
	}
	return h.buf.Snapshot()
}

func HistoryMiddleware[Req, Res any](hist *History[Req, Res]) Middleware[Req, Res] {
	return func(next Fn[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			start := time.Now()
			res, err = next(ctx, req)
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
