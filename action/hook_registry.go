// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nexssp/kernel/xerr"
)

var (
	ErrDuplicateHook  = xerr.Conflict("duplicate hook registration")
	ErrEmptyHookName  = xerr.BadRequest("hook name cannot be empty")
	ErrNilHookFactory = xerr.BadRequest("hook factory cannot be nil")
	ErrHookNotFound   = xerr.NotFound("hook not registered")
)

type HookFactory func() AnyHook

// HookRegistry provides concurrent-safe, lock-free lookups for named hook factories.
type HookRegistry struct {
	mutex  sync.Mutex
	active atomic.Pointer[map[string]HookFactory]
}

func NewHookRegistry() *HookRegistry {
	registry := &HookRegistry{}
	initialState := make(map[string]HookFactory)
	registry.active.Store(&initialState)
	return registry
}

func (registry *HookRegistry) Register(name string, factory HookFactory) error {
	if name == "" {
		return ErrEmptyHookName
	}
	if factory == nil {
		return ErrNilHookFactory
	}

	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	current := registry.active.Load()
	var nextState map[string]HookFactory
	if current != nil {
		if _, exists := (*current)[name]; exists {
			return fmt.Errorf("%w: %s", ErrDuplicateHook, name)
		}
		nextState = make(map[string]HookFactory, len(*current)+1)
		maps.Copy(nextState, *current)
	} else {
		nextState = make(map[string]HookFactory, 1)
	}

	nextState[name] = factory
	registry.active.Store(&nextState)
	return nil
}

func (registry *HookRegistry) NamedHook(name string) (AnyHook, bool) {
	current := registry.active.Load()
	if current == nil {
		return AnyHook{}, false
	}
	factory, exists := (*current)[name]
	if !exists {
		return AnyHook{}, false
	}
	return factory(), true
}

func (registry *HookRegistry) NamedHookNames() []string {
	current := registry.active.Load()
	if current == nil {
		return nil
	}
	names := make([]string, 0, len(*current))
	for name := range *current {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (registry *HookRegistry) Reset() {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	emptyState := make(map[string]HookFactory)
	registry.active.Store(&emptyState)
}

var defaultHookRegistry = NewHookRegistry()

func RegisterHook(name string, factory HookFactory) error {
	return defaultHookRegistry.Register(name, factory)
}

func MustRegisterHook(name string, factory HookFactory) {
	if err := RegisterHook(name, factory); err != nil {
		panic(err)
	}
}

func NamedHook(name string) (AnyHook, bool) {
	return defaultHookRegistry.NamedHook(name)
}

func NamedHookNames() []string {
	return defaultHookRegistry.NamedHookNames()
}

func MustNamedHook(name string) AnyHook {
	hook, found := NamedHook(name)
	if !found {
		panic(fmt.Sprintf("%v: %s", ErrHookNotFound, name))
	}
	return hook
}

func ResetHookRegistry() {
	defaultHookRegistry.Reset()
}
