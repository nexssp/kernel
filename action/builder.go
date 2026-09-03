package action

import (
	"reflect"
	"runtime"
	"slices"
	"time"
)

// Builder[Req, Res] is the fluent construction API for an action.
// All methods return the same builder for chaining.
// Call Build() once — the result is immutable and safe for concurrent use.
type Builder[Req, Res any] struct {
	meta        Meta
	exec        Fn[Req, Res]
	middlewares []DispatcherMiddleware[Req, Res]
	hooks       []Hook[Req, Res]
	anyHooks    []AnyHook
	bindings    []Binding
	history     *History[Req, Res]
}

// New creates an action builder.
func New[Req, Res any](name string, exec Fn[Req, Res]) *Builder[Req, Res] {
	if name == "" {
		name = funcName(exec)
	}
	return &Builder[Req, Res]{
		meta: Meta{Name: name},
		exec: exec,
	}
}

func funcName(exec any) string {
	if exec == nil {
		return "unknown.action"
	}
	pc := reflect.ValueOf(exec).Pointer()
	if fn := runtime.FuncForPC(pc); fn != nil {
		return fn.Name()
	}
	return "unknown.action"
}

// ── Identity ──────────────────────────────────────────────────────────────────

func (b *Builder[Req, Res]) Name(name string) *Builder[Req, Res] {
	b.meta.Name = name
	return b
}

func (b *Builder[Req, Res]) Description(d string) *Builder[Req, Res] {
	b.meta.Description = d
	return b
}

func (b *Builder[Req, Res]) Describe() *Meta {
	return &b.meta
}

func (b *Builder[Req, Res]) Tag(tags ...string) *Builder[Req, Res] {
	b.meta.Tags = append(b.meta.Tags, tags...)
	return b
}

// Public exposes this action through the browser/client business contract.
// It does not weaken authentication or authorization requirements.
func (b *Builder[Req, Res]) Public() *Builder[Req, Res] {
	b.meta.Scope = ScopePublic
	return b
}

// Internal exposes this action only through an explicitly requested trusted
// service-to-service or runner contract.
func (b *Builder[Req, Res]) Internal() *Builder[Req, Res] {
	b.meta.Scope = ScopeInternal
	return b
}

// System marks a framework or operations-plane action. It is excluded from
// public and trusted business contracts.
func (b *Builder[Req, Res]) System() *Builder[Req, Res] {
	b.meta.Scope = ScopeSystem
	return b
}

func (b *Builder[Req, Res]) Node(nodeName string) *Builder[Req, Res] {
	b.meta.Node = nodeName
	return b
}

// ── Transport binding ─────────────────────────────────────────────────────────

func (b *Builder[Req, Res]) Route(bs ...Binding) *Builder[Req, Res] {
	b.bindings = append(b.bindings, bs...)
	return b
}

// ── Idempotency ───────────────────────────────────────────────────────────────

func (b *Builder[Req, Res]) Idempotent() *Builder[Req, Res] {
	b.meta.Idempotency = IdempotencyConfig{Enabled: true}
	return b
}

func (b *Builder[Req, Res]) IdempotentWithConfig(cfg IdempotencyConfig) *Builder[Req, Res] {
	cfg.Enabled = true
	b.meta.Idempotency = cfg
	return b
}

func (b *Builder[Req, Res]) SuccessStatus(code int) *Builder[Req, Res] {
	b.meta.SuccessStatus = code
	return b
}

// ── Middleware ────────────────────────────────────────────────────────────────

func (b *Builder[Req, Res]) Use(m Middleware[Req, Res]) *Builder[Req, Res] {
	b.middlewares = append(b.middlewares, func(next Fn[Req, Res], _ HookDispatcher[Req, Res]) Fn[Req, Res] {
		return m(next)
	})
	return b
}

func (b *Builder[Req, Res]) UseWithDispatcher(m DispatcherMiddleware[Req, Res]) *Builder[Req, Res] {
	b.middlewares = append(b.middlewares, m)
	return b
}

// ── Built-in middleware shorthands ────────────────────────────────────────────

func (b *Builder[Req, Res]) Timeout(d time.Duration) *Builder[Req, Res] {
	b.meta.Timeout = d
	return b.Use(TimeoutMiddleware[Req, Res](d))
}

func (b *Builder[Req, Res]) ConcurrencyLimit(limit int32) *Builder[Req, Res] {
	b.meta.ConcurrencyLimit = limit
	return b.Use(ConcurrencyLimitMiddleware[Req, Res](limit))
}

func (b *Builder[Req, Res]) WithHistory(capacity int) (*Builder[Req, Res], *History[Req, Res]) {
	hist := NewHistory[Req, Res](capacity)
	b.history = hist
	return b.Use(HistoryMiddleware(hist)), hist
}

// ── Build ─────────────────────────────────────────────────────────────────────

func (b *Builder[Req, Res]) Build() *BuiltAction[Req, Res] {
	metaCopy := b.meta
	metaCopy.Tags = slices.Clone(b.meta.Tags)
	metaCopy.RequiredRoles = slices.Clone(b.meta.RequiredRoles)
	metaCopy.RequiredPermissions = slices.Clone(b.meta.RequiredPermissions)
	metaCopy.RequiredFeatures = slices.Clone(b.meta.RequiredFeatures)

	act := &BuiltAction[Req, Res]{
		meta:     &metaCopy,
		hooks:    append([]Hook[Req, Res](nil), b.hooks...),
		bindings: append([]Binding(nil), b.bindings...),
		history:  b.history,
	}
	act.anyHooks.Store(&anyHookSet{hooks: append([]AnyHook(nil), b.anyHooks...)})

	exec := b.exec
	if exec != nil {
		for _, m := range slices.Backward(b.middlewares) {
			exec = m(exec, act)
		}
	}
	act.exec = exec

	for _, h := range act.anyHooksSnapshot() {
		if h.OnRegister != nil {
			h.OnRegister(act.meta)
		}
	}
	for _, h := range act.hooks {
		if h.OnRegister != nil {
			h.OnRegister(act.meta)
		}
	}

	return act
}

func (b *Builder[Req, Res]) LogSlowWhen(d time.Duration) *Builder[Req, Res] {
	b.meta.LogSlowThreshold = d
	if d > 0 {
		return b.Use(SlowLogMiddleware[Req, Res](d, b.meta.Name))
	}
	return b
}
