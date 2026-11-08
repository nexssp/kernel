// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"time"

	"github.com/nexssp/kernel/ringbuf"
	"github.com/nexssp/kernel/xerr"
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

// NewHistory creates a bounded execution history. It returns nil if the
// allocation exceeds the default memory budget; use NewHistoryChecked to
// inspect errors or NewHistoryWithOptions to select an explicit budget.
func NewHistory[Req, Res any](capacity int) *History[Req, Res] {
	history, err := NewHistoryChecked[Req, Res](capacity)
	if err != nil {
		return nil
	}
	return history
}

// NewHistoryChecked creates a bounded execution history using the default
// memory budget and returns an error before allocating when the budget is
// exceeded or the backing array size is not representable.
func NewHistoryChecked[Req, Res any](capacity int) (*History[Req, Res], error) {
	return NewHistoryWithOptions[Req, Res](ringbuf.BufferOptions{
		Capacity:          capacity,
		MemoryBudgetBytes: ringbuf.DefaultMemoryBudgetBytes,
	})
}

// NewHistoryWithOptions creates a bounded execution history using explicit
// buffer options and returns an error if allocation validation fails.
func NewHistoryWithOptions[Req, Res any](options ringbuf.BufferOptions) (*History[Req, Res], error) {
	buffer, err := ringbuf.NewBufferWithOptions[Record[Req, Res]](options)
	if err != nil {
		return nil, err
	}
	return &History[Req, Res]{buf: buffer}, nil
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
			defer func() {
				rec := Record[Req, Res]{
					Time:     start,
					Duration: time.Since(start),
					Req:      req,
					Res:      res,
					Err:      err,
				}
				if r := recover(); r != nil {
					// Record panicking calls, then re-panic.
					var zero Res
					rec.Res = zero
					rec.Err = xerr.PanicRecovery(r)
					hist.Push(rec)
					panic(r)
				}
				hist.Push(rec)
			}()
			res, err = next(ctx, req)
			return res, err
		}
	}
}
