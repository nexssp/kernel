package action

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Compile-time interface verifications
var (
	_ AnyAction = (*Proxy)(nil)
	_ Composite = (*Proxy)(nil)
)

// Proxy wraps an AnyAction and allows it to be atomically hot-swapped at runtime
// with zero downtime and zero locks on the hot path.
type Proxy struct {
	active atomic.Value
}

// NewProxy creates a new hot-swappable proxy initialized with a starting action.
func NewProxy(initial AnyAction) *Proxy {
	p := &Proxy{}
	if initial != nil {
		p.active.Store(initial)
	}
	return p
}

// Current returns the active action or nil if uninitialized.
func (p *Proxy) Current() AnyAction {
	v := p.active.Load()
	if v == nil {
		return nil
	}
	return v.(AnyAction)
}

// Swap atomically replaces the underlying action with a new one.
func (p *Proxy) Swap(newAction AnyAction) {
	if newAction != nil {
		p.active.Store(newAction)
	}
}

// DoAny delegates execution to the currently active action.
func (p *Proxy) DoAny(ctx context.Context, req any) (any, error) {
	act := p.Current()
	if act == nil {
		return nil, fmt.Errorf("action: proxy has no active action")
	}
	return act.DoAny(ctx, req)
}

// ExecuteDecoded delegates execution using the kernel's DecodeFunc type.
func (p *Proxy) ExecuteDecoded(ctx context.Context, decodeFn DecodeFunc) (any, error) {
	act := p.Current()
	if act == nil {
		return nil, fmt.Errorf("action: proxy has no active action")
	}
	return act.ExecuteDecoded(ctx, decodeFn)
}

// Describe delegates metadata description to the active action.
func (p *Proxy) Describe() *Meta {
	act := p.Current()
	if act == nil {
		return &Meta{Name: "proxy.uninitialized"}
	}
	return act.Describe()
}

// GetBindings returns typed bindings from the active action.
func (p *Proxy) GetBindings() []Binding {
	act := p.Current()
	if act == nil {
		return nil
	}
	return act.GetBindings()
}

// AddAnyHook applies variadic hooks to the active action.
func (p *Proxy) AddAnyHook(hooks ...AnyHook) {
	if act := p.Current(); act != nil {
		act.AddAnyHook(hooks...)
	}
}

// GetAnyHooks returns the hooks from the active action.
func (p *Proxy) GetAnyHooks() []AnyHook {
	act := p.Current()
	if act == nil {
		return nil
	}
	return act.GetAnyHooks()
}

// Children satisfies the Composite interface by unwrapping the active action.
func (p *Proxy) Children() []AnyAction {
	if comp, ok := p.Current().(Composite); ok {
		return comp.Children()
	}
	return nil
}
