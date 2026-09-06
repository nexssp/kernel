// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE fil
package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nexssp/kernel/xerr"
)

var ErrTypeAssertion = errors.New("critical type assertion failure")

type execStateKey struct{}

type execState struct {
	fromCache bool
}

type BuiltAction[Req, Res any] struct {
	meta *Meta
	exec Fn[Req, Res]

	hooks      []Hook[Req, Res]
	anyHooks   atomic.Pointer[anyHookSet]
	anyHooksMu sync.Mutex
	bindings   []Binding
	history    *History[Req, Res]
}

type anyHookSet struct {
	hooks []AnyHook
}

func (a *BuiltAction[Req, Res]) GetMeta() *Meta {
	if a.meta == nil {
		return nil
	}
	cp := *a.meta
	cp.Tags = slices.Clone(a.meta.Tags)
	cp.RequiredRoles = slices.Clone(a.meta.RequiredRoles)
	cp.RequiredPermissions = slices.Clone(a.meta.RequiredPermissions)
	cp.RequiredFeatures = slices.Clone(a.meta.RequiredFeatures)
	return &cp
}

func (a *BuiltAction[Req, Res]) GetBindings() []Binding {
	return append([]Binding(nil), a.bindings...)
}

func (a *BuiltAction[Req, Res]) GetAnyHooks() []AnyHook {
	return append([]AnyHook(nil), a.anyHooksSnapshot()...)
}

func (a *BuiltAction[Req, Res]) Describe() *Meta {
	return a.GetMeta()
}

func (a *BuiltAction[Req, Res]) History() *History[Req, Res] {
	return a.history
}

func (a *BuiltAction[Req, Res]) anyHooksSnapshot() []AnyHook {
	set := a.anyHooks.Load()
	if set == nil {
		return nil
	}
	return set.hooks
}

func hasOnExecutedHook[Req, Res any](hooks []Hook[Req, Res], anyHooks []AnyHook) bool {
	for i := range hooks {
		if hooks[i].OnExecuted != nil {
			return true
		}
	}
	for i := range anyHooks {
		if anyHooks[i].OnExecuted != nil {
			return true
		}
	}
	return false
}

func (a *BuiltAction[Req, Res]) Do(ctx context.Context, req Req) (res Res, err error) {
	var anyHooksRan, typedHooksRan int

	anyHooks := a.anyHooksSnapshot()

	needExecState := hasOnExecutedHook(a.hooks, anyHooks)

	var state *execState
	finalCtx := ctx
	if needExecState {
		state = &execState{}
		finalCtx = context.WithValue(ctx, execStateKey{}, state)
	}

	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
			for _, h := range anyHooks {
				if h.OnPanic != nil {
					h := h
					callHook(a.meta, "OnPanic", func() {
						h.OnPanic(ctx, any(req), r, a.meta)
					})
				}
			}
		}

		for i := typedHooksRan - 1; i >= 0; i-- {
			h := a.hooks[i]

			switch {
			case err != nil && errors.Is(err, context.Canceled):
				if h.OnCancel != nil {
					h := h
					callHook(a.meta, "OnCancel", func() {
						h.OnCancel(finalCtx, req, a.meta)
					})
				}
			case err != nil:
				if h.OnError != nil {
					h := h
					callHook(a.meta, "OnError", func() {
						h.OnError(finalCtx, req, err, a.meta)
					})
				}
			case h.OnExecuted != nil && state != nil && !state.fromCache:
				h := h
				callHook(a.meta, "OnExecuted", func() {
					h.OnExecuted(finalCtx, req, res, nil, a.meta)
				})
			}

			if h.After != nil {
				h := h
				callHook(a.meta, "After", func() {
					h.After(finalCtx, req, res, err, a.meta)
				})
			}
		}

		for i := anyHooksRan - 1; i >= 0; i-- {
			h := anyHooks[i]

			switch {
			case errors.Is(err, context.Canceled):
				if h.OnCancel != nil {
					h := h
					callHook(a.meta, "OnCancel", func() {
						h.OnCancel(finalCtx, any(req), a.meta)
					})
				}
			case err != nil:
				if h.OnError != nil {
					h := h
					callHook(a.meta, "OnError", func() {
						h.OnError(finalCtx, any(req), err, a.meta)
					})
				}
			case h.OnExecuted != nil && state != nil && !state.fromCache:
				h := h
				callHook(a.meta, "OnExecuted", func() {
					h.OnExecuted(finalCtx, any(req), any(res), nil, a.meta)
				})
			}

			if h.After != nil {
				h := h
				callHook(a.meta, "After", func() {
					h.After(finalCtx, any(req), any(res), err, a.meta)
				})
			}
		}
	}()

	var perr error
	for _, h := range anyHooks {
		if h.Before != nil {
			finalCtx, perr = h.Before(finalCtx, any(req), a.meta)
			if perr != nil {
				return res, fmt.Errorf("action %s before-hook failed: %w", a.meta.Name, perr)
			}
		}
		anyHooksRan++
	}

	for _, h := range a.hooks {
		if h.Before != nil {
			finalCtx, perr = h.Before(finalCtx, req, a.meta)
			if perr != nil {
				return res, fmt.Errorf("action %s before-hook failed: %w", a.meta.Name, perr)
			}
		}
		typedHooksRan++
	}

	if a.exec == nil {
		return res, nil
	}

	res, err = a.exec(finalCtx, req)
	if err != nil {
		return res, fmt.Errorf("action %s execution failed: %w", a.meta.Name, err)
	}

	return res, nil
}

func (a *BuiltAction[Req, Res]) OnCacheHit(ctx context.Context, req Req, res Res) {
	if s, ok := ctx.Value(execStateKey{}).(*execState); ok && s != nil {
		s.fromCache = true
	}

	for _, h := range a.hooks {
		if h.OnCacheHit != nil {
			h.OnCacheHit(ctx, req, res, a.meta)
		}
	}
	for _, h := range a.anyHooksSnapshot() {
		if h.OnCacheHit != nil {
			h.OnCacheHit(ctx, any(req), any(res), a.meta)
		}
	}
}

func (a *BuiltAction[Req, Res]) OnCacheMiss(ctx context.Context, req Req) {
	for _, h := range a.hooks {
		if h.OnCacheMiss != nil {
			h.OnCacheMiss(ctx, req, a.meta)
		}
	}
	for _, h := range a.anyHooksSnapshot() {
		if h.OnCacheMiss != nil {
			h.OnCacheMiss(ctx, any(req), a.meta)
		}
	}
}

func (a *BuiltAction[Req, Res]) OnRetry(ctx context.Context, req Req, attempt int, err error) {
	for _, h := range a.hooks {
		if h.OnRetry != nil {
			h.OnRetry(ctx, req, attempt, err, a.meta)
		}
	}
	for _, h := range a.anyHooksSnapshot() {
		if h.OnRetry != nil {
			h.OnRetry(ctx, any(req), attempt, err, a.meta)
		}
	}
}

func (a *BuiltAction[Req, Res]) OnCoalesced(ctx context.Context, req Req) {
	for _, h := range a.hooks {
		if h.OnCoalesced != nil {
			h.OnCoalesced(ctx, req, a.meta)
		}
	}
	for _, h := range a.anyHooksSnapshot() {
		if h.OnCoalesced != nil {
			h.OnCoalesced(ctx, any(req), a.meta)
		}
	}
}

func (a *BuiltAction[Req, Res]) OnDeduplicated(ctx context.Context, req Req) {
	for _, h := range a.hooks {
		if h.OnDeduplicated != nil {
			h.OnDeduplicated(ctx, req, a.meta)
		}
	}
	for _, h := range a.anyHooksSnapshot() {
		if h.OnDeduplicated != nil {
			h.OnDeduplicated(ctx, any(req), a.meta)
		}
	}
}

func (a *BuiltAction[Req, Res]) ExecuteDecoded(ctx context.Context, decode DecodeFunc) (any, error) {
	if decode == nil {
		var zero Req
		return a.Do(ctx, zero)
	}

	rt := reflect.TypeFor[Req]()

	if rt.Kind() == reflect.Pointer {
		reqVal := reflect.New(rt.Elem()).Interface()
		req, ok := reqVal.(Req)
		if !ok {
			return nil, fmt.Errorf("%w for pointer type %T", ErrTypeAssertion, reqVal)
		}
		if err := decode(req); err != nil {
			return nil, fmt.Errorf("decode request failed: %w", err)
		}
		return a.Do(ctx, req)
	}

	var req Req
	if err := decode(&req); err != nil {
		return nil, fmt.Errorf("decode request failed: %w", err)
	}
	return a.Do(ctx, req)
}

func (a *BuiltAction[Req, Res]) AddAnyHook(h ...AnyHook) {
	if len(h) == 0 {
		return
	}

	a.anyHooksMu.Lock()
	defer a.anyHooksMu.Unlock()

	current := a.anyHooksSnapshot()
	next := make([]AnyHook, 0, len(current)+len(h))
	next = append(next, current...)
	for _, hook := range h {
		if hook.OnRegister != nil {
			hook.OnRegister(a.meta)
		}
		next = append(next, hook)
	}
	a.anyHooks.Store(&anyHookSet{hooks: next})
}

func (a *BuiltAction[Req, Res]) ReqPayload() any { var r Req; return r }
func (a *BuiltAction[Req, Res]) ResPayload() any { var r Res; return r }

func callHook(meta *Meta, hook string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("action_hook_panic",
				"action", meta.Name,
				"hook", hook,
				"panic", r,
			)
		}
	}()
	fn()
}

// InvokeAny executes an action with an in-memory input payload.
// If act implements Invoker (e.g. BuiltAction), it runs DoAny directly (0 allocs on matching types).
// If act only implements Executable (e.g. custom transport action), it decodes safely.
func InvokeAny(ctx context.Context, act any, req any) (any, error) {
	if act == nil {
		return nil, errors.New("action: cannot invoke nil action")
	}

	// ⚡ Fast path: action implements Invoker (BuiltAction)
	if invoker, ok := act.(AnyDoer); ok {
		return invoker.DoAny(ctx, req)
	}

	// Fallback for types implementing only Executable
	if exec, ok := act.(Executable); ok {
		return exec.ExecuteDecoded(ctx, func(target any) error {
			if req == nil {
				return nil
			}
			// Use fast Coerce instead of blind json.Marshal
			return copyInto(target, req)
		})
	}

	return nil, fmt.Errorf("action: type %T does not implement Invoker or Executable", act)
}

func copyInto(target any, source any) error {
	rt := reflect.TypeOf(target)
	if rt != nil && rt.Kind() == reflect.Pointer {
		sourceVal := reflect.ValueOf(source)
		targetVal := reflect.ValueOf(target)
		if sourceVal.IsValid() && sourceVal.Type().AssignableTo(targetVal.Elem().Type()) {
			targetVal.Elem().Set(sourceVal)
			return nil
		}
	}
	// Fallback serialization only when types completely differ
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
