// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"errors"
	"iter"
	"sync/atomic"
)

type typedStreamProxyState[Req, Item any] struct {
	action *StreamAction[Req, Item]
}

// TypedStreamProxy guarantees zero-allocation hot-swapping for strictly typed stream chains.
// It bypasses the AnyStream boundaries entirely.
type TypedStreamProxy[Req, Item any] struct {
	active atomic.Value // typedStreamProxyState
}

// NewTypedStreamProxy creates a strictly typed proxy with an optional initial stream.
func NewTypedStreamProxy[Req, Item any](initial *StreamAction[Req, Item]) *TypedStreamProxy[Req, Item] {
	p := &TypedStreamProxy[Req, Item]{}
	if initial != nil {
		p.active.Store(typedStreamProxyState[Req, Item]{action: initial})
	}
	return p
}

// Current returns the active strongly typed stream action, or nil.
func (p *TypedStreamProxy[Req, Item]) Current() *StreamAction[Req, Item] {
	if p == nil {
		return nil
	}
	v := p.active.Load()
	if v == nil {
		return nil
	}
	state, ok := v.(typedStreamProxyState[Req, Item])
	if !ok {
		return nil
	}
	return state.action
}

// Swap atomically replaces the implementation.
func (p *TypedStreamProxy[Req, Item]) Swap(next *StreamAction[Req, Item]) {
	if p != nil && next != nil {
		p.active.Store(typedStreamProxyState[Req, Item]{action: next})
	}
}

// Do executes the typed pipeline bypassing type erasure limits.
func (p *TypedStreamProxy[Req, Item]) Do(ctx context.Context, req Req) (iter.Seq2[Item, error], error) {
	act := p.Current()
	if act == nil {
		return nil, errors.New("action: typed stream proxy has no active action")
	}
	return act.Do(ctx, req)
}
