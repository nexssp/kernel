package action

import (
	"context"
	"errors"
	"sync/atomic"
)

var _ AnyStreamAction = (*StreamProxy)(nil)

type streamProxyState struct {
	action AnyStreamAction
}

// StreamProxy wraps an AnyStreamAction and atomically hot-swaps the
// implementation used by new stream invocations. An already-created stream
// continues using the implementation selected when DoStreamAny was called.
type StreamProxy struct {
	active atomic.Value // streamProxyState
}

// NewStreamProxy creates a proxy with an optional initial stream action.
func NewStreamProxy(initial AnyStreamAction) *StreamProxy {
	p := &StreamProxy{}
	if initial != nil {
		p.active.Store(streamProxyState{action: initial})
	}
	return p
}

// Current returns the active stream action, or nil when uninitialized.
func (p *StreamProxy) Current() AnyStreamAction {
	if p == nil {
		return nil
	}
	v := p.active.Load()
	if v == nil {
		return nil
	}
	state, ok := v.(streamProxyState)
	if !ok {
		return nil
	}
	return state.action
}

// Swap atomically replaces the implementation used by future invocations.
// A nil action is ignored so an accidental nil update cannot disable a live
// stream endpoint.
func (p *StreamProxy) Swap(next AnyStreamAction) {
	if p != nil && next != nil {
		p.active.Store(streamProxyState{action: next})
	}
}

// DoStreamAny delegates to the action selected at invocation time.
func (p *StreamProxy) DoStreamAny(ctx context.Context, req any) (AnyStream, error) {
	act := p.Current()
	if act == nil {
		return nil, errors.New("action: stream proxy has no active action")
	}
	return act.DoStreamAny(ctx, req)
}

// Describe delegates metadata lookup to the active stream action.
func (p *StreamProxy) Describe() *Meta {
	if act := p.Current(); act != nil {
		return act.Describe()
	}
	return &Meta{Name: "stream_proxy.uninitialized"}
}

// ReqPayload returns the active stream request payload prototype.
func (p *StreamProxy) ReqPayload() any {
	if act := p.Current(); act != nil {
		return act.ReqPayload()
	}
	return nil
}

// ResPayload returns the active stream item payload prototype.
func (p *StreamProxy) ResPayload() any {
	if act := p.Current(); act != nil {
		return act.ResPayload()
	}
	return nil
}

// GetBindings delegates routing metadata to the active stream action.
func (p *StreamProxy) GetBindings() []Binding {
	if act := p.Current(); act != nil {
		return act.GetBindings()
	}
	return nil
}

// ── Interfejs AnyStreamAction (AnyHook Support) ──────────────────────────────

func (p *StreamProxy) GetAnyHooks() []AnyHook {
	if act := p.Current(); act != nil {
		return act.GetAnyHooks()
	}
	return nil
}

func (p *StreamProxy) AddAnyHook(hooks ...AnyHook) {
	if act := p.Current(); act != nil {
		act.AddAnyHook(hooks...)
	}
}

func (p *StreamProxy) CloneWithHooks(hooks ...AnyHook) AnyStreamAction {
	if p == nil {
		return nil
	}
	act := p.Current()
	if act == nil {
		return p
	}
	cloned := act.CloneWithHooks(hooks...)
	if cloned == nil {
		return p
	}
	return NewStreamProxy(cloned)
}
