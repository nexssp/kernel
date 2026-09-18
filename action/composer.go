// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// ── Pipe: A → B → C ────────────────────────────────────────────────────────────

// Pipe connects two actions: output of first feeds input of second.
// Usage:
//
//	pipe := action.Pipe[Req, Middle, Res]("order.pipe",
//	    buildCheck, buildProcess)
//	result := pipe.Do(ctx, req)
func Pipe[Req, Mid, Res any](
	name string,
	first *BuiltAction[Req, Mid],
	second *BuiltAction[Mid, Res],
) *Builder[Req, Res] {
	desc := fmt.Sprintf("Pipe: %s -> %s", first.Describe().Name, second.Describe().Name)
	b := New(name, func(ctx context.Context, req Req) (Res, error) {
		mid, err := first.Do(ctx, req)
		if err != nil {
			var zero Res
			return zero, fmt.Errorf("pipe first step [%s] failed: %w", first.Describe().Name, err)
		}
		res, err := second.Do(ctx, mid)
		if err != nil {
			var zero Res
			return zero, fmt.Errorf("pipe second step [%s] failed: %w", second.Describe().Name, err)
		}
		return res, nil
	}).
		Description(desc).
		Tag("pipe", "composer")

	// Merge unique tags from both child actions
	for _, t := range append(first.Describe().Tags, second.Describe().Tags...) {
		if !slices.Contains(b.meta.Tags, t) {
			b.Tag(t)
		}
	}
	return b
}

// Branch routes a request to one of several named actions based on a router function.
func Branch[Req, Res any](
	name string,
	routes map[string]*Builder[Req, Res],
	router func(context.Context, Req) (string, error),
) *Builder[Req, Res] {
	built := make(map[string]*BuiltAction[Req, Res], len(routes))
	var allTags []string
	for key, b := range routes {
		builtAction := b.Build()
		built[key] = builtAction
		allTags = append(allTags, builtAction.Describe().Tags...)
	}

	b := New(name, func(ctx context.Context, req Req) (Res, error) {
		key, err := router(ctx, req)
		if err != nil {
			var zero Res
			return zero, fmt.Errorf("branch router failed: %w", err)
		}
		act, ok := built[key]
		if !ok {
			var zero Res
			return zero, fmt.Errorf("branch %q: no route for key %q", name, key)
		}
		return act.Do(ctx, req)
	}).
		Description("Branch router for: "+name).
		Tag("branch", "composer")

	for _, t := range allTags {
		if !slices.Contains(b.meta.Tags, t) {
			b.Tag(t)
		}
	}
	return b
}

// Parallel executes all actions concurrently with the same request.
// Concurrency pattern: Scatter-gather using sync.WaitGroup with independent result slots.
func Parallel[Req, Res any](
	name string,
	builders ...*Builder[Req, Res],
) *Builder[Req, []Res] {
	if len(builders) == 0 {
		return New(name, func(_ context.Context, _ Req) ([]Res, error) {
			return nil, nil
		})
	}

	acts := make([]*BuiltAction[Req, Res], len(builders))
	for i, b := range builders {
		acts[i] = b.Build()
	}

	return New(name, func(ctx context.Context, req Req) ([]Res, error) {
		results := make([]Res, len(acts))
		errs := make([]error, len(acts))
		var wg sync.WaitGroup

		for i, a := range acts {
			wg.Add(1)
			go func(idx int, act *BuiltAction[Req, Res]) {
				defer wg.Done()
				res, err := act.Do(ctx, req)
				results[idx] = res
				errs[idx] = err
			}(i, a)
		}
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				return results, fmt.Errorf("parallel action [%s] failed: %w", acts[i].Describe().Name, err)
			}
		}
		return results, nil
	}).
		Description(fmt.Sprintf("Parallel execution of %d actions", len(acts))).
		Tag("parallel", "composer")
}

// FirstSuccess executes actions in order, returning the first non-error result.
func FirstSuccess[Req, Res any](
	name string,
	builders ...*Builder[Req, Res],
) *Builder[Req, Res] {
	built := make([]*BuiltAction[Req, Res], len(builders))
	for i, b := range builders {
		built[i] = b.Build()
	}

	return New(name, func(ctx context.Context, req Req) (Res, error) {
		var lastErr error
		for _, act := range built {
			res, err := act.Do(ctx, req)
			if err == nil {
				return res, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				var zero Res
				return zero, fmt.Errorf("first success context canceled: %w", ctx.Err())
			}
		}
		var zero Res
		return zero, fmt.Errorf("all actions failed in FirstSuccess, last error: %w", lastErr)
	}).Description("First success fallback chain").Tag("fallback", "composer")
}

// Chain runs same-typed actions sequentially; each receives the previous output.
func Chain[T any](
	name string,
	builders ...*Builder[T, T],
) *Builder[T, T] {
	acts := make([]*BuiltAction[T, T], len(builders))
	for i, b := range builders {
		acts[i] = b.Build()
	}

	return New(name, func(ctx context.Context, req T) (T, error) {
		cur := req
		for _, act := range acts {
			var err error
			cur, err = act.Do(ctx, cur)
			if err != nil {
				return cur, fmt.Errorf("chain step [%s] failed: %w", act.Describe().Name, err)
			}
		}
		return cur, nil
	}).
		Description("Sequential pipeline chain").
		Tag("chain", "composer")
}

// PipeWith connects two actions whose types don't match 1:1 by providing an inline
// transformation function from Mid1 to Mid2.
// Eliminates throwaway adapter actions and runs with 0 allocations on field extraction.
func PipeWith[Req, Mid1, Mid2, Res any](
	name string,
	first *BuiltAction[Req, Mid1],
	transform func(context.Context, Mid1) (Mid2, error),
	second *BuiltAction[Mid2, Res],
) *Builder[Req, Res] {
	desc := fmt.Sprintf("PipeWith: %s -> [transform] -> %s", first.Describe().Name, second.Describe().Name)
	b := New(name, func(ctx context.Context, req Req) (Res, error) {
		mid1, err := first.Do(ctx, req)
		if err != nil {
			var zero Res
			return zero, fmt.Errorf("pipe first step [%s] failed: %w", first.Describe().Name, err)
		}
		mid2, err := transform(ctx, mid1)
		if err != nil {
			var zero Res
			return zero, fmt.Errorf("pipe transform [%s -> %s] failed: %w", first.Describe().Name, second.Describe().Name, err)
		}
		res, err := second.Do(ctx, mid2)
		if err != nil {
			var zero Res
			return zero, fmt.Errorf("pipe second step [%s] failed: %w", second.Describe().Name, err)
		}
		return res, nil
	}).
		Description(desc).
		Tag("pipe", "transform", "composer")

	for _, t := range append(first.Describe().Tags, second.Describe().Tags...) {
		if !slices.Contains(b.meta.Tags, t) {
			b.Tag(t)
		}
	}
	return b
}

// ParallelNamed runs heterogeneous actions concurrently and returns results in a map keyed by branch name.
// Concurrency pattern: Scatter-gather using sync.WaitGroup with panic isolation on each branch.
func ParallelNamed[Req any](
	name string,
	routes map[string]AnyAction,
) *Builder[Req, map[string]any] {
	if len(routes) == 0 {
		return New(name, func(_ context.Context, _ Req) (map[string]any, error) {
			return make(map[string]any), nil
		})
	}

	var allTags []string
	for _, a := range routes {
		if a != nil {
			allTags = append(allTags, a.Describe().Tags...)
		}
	}

	b := New(name, func(ctx context.Context, req Req) (map[string]any, error) {
		results := make(map[string]any, len(routes))
		errs := make(map[string]error, len(routes))
		var mu sync.Mutex
		var wg sync.WaitGroup

		for key, a := range routes {
			if a == nil {
				continue
			}
			wg.Add(1)
			go func(branchKey string, act AnyAction) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						mu.Lock()
						errs[branchKey] = fmt.Errorf("panic in parallel branch [%s]: %v", branchKey, r)
						mu.Unlock()
					}
				}()

				res, err := InvokeAny(ctx, act, req)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errs[branchKey] = err
					return
				}
				results[branchKey] = res
			}(key, a)
		}
		wg.Wait()

		for branch, err := range errs {
			if err != nil {
				return results, fmt.Errorf("parallel branch [%s] failed: %w", branch, err)
			}
		}
		return results, nil
	}).
		Description(fmt.Sprintf("Heterogeneous parallel execution of %d branches", len(routes))).
		Tag("parallel", "named", "composer")

	for _, t := range allTags {
		if !slices.Contains(b.meta.Tags, t) {
			b.Tag(t)
		}
	}
	return b
}

// ParallelMap runs heterogeneous actions concurrently, keying the output map by each action's declared Name().
func ParallelMap[Req any](
	name string,
	actions ...AnyAction,
) *Builder[Req, map[string]any] {
	routes := make(map[string]AnyAction, len(actions))
	for _, a := range actions {
		if a != nil {
			routes[a.Describe().Name] = a
		}
	}
	return ParallelNamed[Req](name, routes)
}

// BranchAny routes dynamic execution to one of several named actions based on a router function.
func BranchAny(
	name string,
	routes map[string]AnyAction,
	router func(context.Context, any) (string, error),
) *Builder[any, any] {
	var allTags []string
	for _, act := range routes {
		if act != nil {
			allTags = append(allTags, act.Describe().Tags...)
		}
	}

	b := New(name, func(ctx context.Context, req any) (any, error) {
		key, err := router(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("branch router failed: %w", err)
		}
		act, ok := routes[key]
		if !ok {
			return nil, fmt.Errorf("branch %q: no route for key %q", name, key)
		}
		return InvokeAny(ctx, act, req)
	}).
		Description("Dynamic branch router for: "+name).
		Tag("branch", "dynamic", "composer")

	for _, t := range allTags {
		if !slices.Contains(b.meta.Tags, t) {
			b.Tag(t)
		}
	}
	return b
}
