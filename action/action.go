// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nexssp/kernel/xctx"
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
	cleanups   []func()
	closeOnce  sync.Once

	// Set once NewRegistry applies library hooks. A second NewRegistry call
	// that tries to apply hooks to the same instance fails loudly instead of
	// silently doubling them.
	registryHooksClaimed atomic.Bool
}

type anyHookSet struct {
	hooks []AnyHook
}

// Close releases resources owned by the action, such as internally-created
// cache janitors. It is safe to call multiple times.
func (a *BuiltAction[Req, Res]) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		for _, cleanup := range a.cleanups {
			if cleanup != nil {
				cleanup()
			}
		}
	})
}

// claimRegistryHooks atomically claims the right to receive registry-level
// hooks. Returns true on the first call, false on every subsequent call.
func (a *BuiltAction[Req, Res]) claimRegistryHooks() bool {
	return a.registryHooksClaimed.CompareAndSwap(false, true)
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
	anyHooks := a.anyHooksSnapshot()
	needExecState := hasOnExecutedHook(a.hooks, anyHooks)

	var state *execState
	finalCtx := setupExecutionContext(ctx, &state, needExecState)

	var anyHooksRan, typedHooksRan int
	var perr error

	// Deferred: panic, then unwinding hooks in reverse.
	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
			firePanicHooks(finalCtx, anyHooks, req, r, a.meta)
		}
		fireExitHooks(finalCtx, a.hooks, anyHooks, req, res, err, state,
			typedHooksRan, anyHooksRan, a.meta)
	}()

	if finalCtx, anyHooksRan, perr = runAnyBeforeHooks(finalCtx, anyHooks, req, a.meta); perr != nil {
		return res, fmt.Errorf("action %s before-hook failed: %w", a.meta.Name, perr)
	}
	if finalCtx, typedHooksRan, perr = runTypedBeforeHooks(finalCtx, a.hooks, req, a.meta); perr != nil {
		return res, fmt.Errorf("action %s before-hook failed: %w", a.meta.Name, perr)
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

func setupExecutionContext(ctx context.Context, state **execState, needExecState bool) context.Context {
	finalCtx := ctx
	if s := xctx.ScopeFrom(finalCtx); s != nil && s.ExecutionID != "" && s.RootExecutionID == "" {
		finalCtx = xctx.WithRootExecutionID(finalCtx, s.ExecutionID)
	}
	if needExecState {
		*state = &execState{}
		finalCtx = context.WithValue(finalCtx, execStateKey{}, *state)
	}
	return finalCtx
}

func runAnyBeforeHooks[Req any](
	ctx context.Context,
	anyHooks []AnyHook,
	req Req,
	meta *Meta,
) (context.Context, int, error) {
	for i, h := range anyHooks {
		if h.Before == nil {
			continue
		}
		var err error
		//nolint:fatcontext // intentional: hook chain must propagate context to next hook and to exec
		ctx, err = h.Before(ctx, any(req), meta)
		if err != nil {
			return ctx, i, err
		}
	}
	return ctx, len(anyHooks), nil
}

func runTypedBeforeHooks[Req, Res any](
	ctx context.Context,
	hooks []Hook[Req, Res],
	req Req,
	meta *Meta,
) (context.Context, int, error) {
	for i, h := range hooks {
		if h.Before == nil {
			continue
		}
		var err error
		//nolint:fatcontext // intentional: hook chain must propagate context to next hook and to exec
		ctx, err = h.Before(ctx, req, meta)
		if err != nil {
			return ctx, i, err
		}
	}
	return ctx, len(hooks), nil
}

func firePanicHooks[Req any](
	ctx context.Context,
	anyHooks []AnyHook,
	req Req,
	recovered any,
	meta *Meta,
) {
	for _, h := range anyHooks {
		if h.OnPanic == nil {
			continue
		}

		callHook(meta, "OnPanic", func() {
			h.OnPanic(ctx, any(req), recovered, meta)
		})
	}
}

func fireExitHooks[Req, Res any](
	ctx context.Context,
	typedHooks []Hook[Req, Res],
	anyHooks []AnyHook,
	req Req,
	res Res,
	err error,
	state *execState,
	typedHooksRan, anyHooksRan int,
	meta *Meta,
) {
	for i := typedHooksRan - 1; i >= 0; i-- {
		fireTypedExitHook(ctx, typedHooks[i], req, res, err, state, meta)
	}
	for i := anyHooksRan - 1; i >= 0; i-- {
		fireAnyExitHook(ctx, anyHooks[i], req, res, err, state, meta)
	}
}

func fireTypedExitHook[Req, Res any](
	ctx context.Context,
	h Hook[Req, Res],
	req Req,
	res Res,
	err error,
	state *execState,
	meta *Meta,
) {
	switch {
	case err != nil && errors.Is(err, context.Canceled):
		if h.OnCancel != nil {
			callHook(meta, "OnCancel", func() { h.OnCancel(ctx, req, meta) })
		}
	case err != nil:
		if h.OnError != nil {
			callHook(meta, "OnError", func() { h.OnError(ctx, req, err, meta) })
		}
	case h.OnExecuted != nil && state != nil && !state.fromCache:
		callHook(meta, "OnExecuted", func() { h.OnExecuted(ctx, req, res, nil, meta) })
	}
	if h.After != nil {
		callHook(meta, "After", func() { h.After(ctx, req, res, err, meta) })
	}
}

func fireAnyExitHook[Req, Res any](
	ctx context.Context,
	h AnyHook,
	req Req,
	res Res,
	err error,
	state *execState,
	meta *Meta,
) {
	switch {
	case errors.Is(err, context.Canceled):
		if h.OnCancel != nil {
			callHook(meta, "OnCancel", func() { h.OnCancel(ctx, any(req), meta) })
		}
	case err != nil:
		if h.OnError != nil {
			callHook(meta, "OnError", func() { h.OnError(ctx, any(req), err, meta) })
		}
	case h.OnExecuted != nil && state != nil && !state.fromCache:
		callHook(meta, "OnExecuted", func() { h.OnExecuted(ctx, any(req), any(res), nil, meta) })
	}
	if h.After != nil {
		callHook(meta, "After", func() { h.After(ctx, any(req), any(res), err, meta) })
	}
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

	reqType := reflect.TypeFor[Req]()
	resType := reflect.TypeFor[Res]()

	applicable := make([]AnyHook, 0, len(h))
	for _, hook := range h {
		if hook.OnBuild != nil && !hook.OnBuild(a.meta, reqType, resType) {
			continue
		}
		applicable = append(applicable, hook)
	}
	if len(applicable) == 0 {
		return
	}

	a.anyHooksMu.Lock()
	defer a.anyHooksMu.Unlock()

	current := a.anyHooksSnapshot()
	next := make([]AnyHook, 0, len(current)+len(applicable))
	next = append(next, current...)
	next = append(next, applicable...)
	a.anyHooks.Store(&anyHookSet{hooks: next})
}

func (a *BuiltAction[Req, Res]) ReqPayload() any { var r Req; return r }
func (a *BuiltAction[Req, Res]) ResPayload() any { var r Res; return r }
