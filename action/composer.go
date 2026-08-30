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
	return New(name, func(ctx context.Context, req T) (T, error) {
		cur := req
		for _, b := range builders {
			act := b.Build()
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
