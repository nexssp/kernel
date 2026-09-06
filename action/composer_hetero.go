package action

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

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
