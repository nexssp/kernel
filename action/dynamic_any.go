package action

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"slices"
	"sync/atomic"

	"github.com/nexssp/kernel/xerr"
)

func PipeAny(name string, left, right AnyAction) *Builder[any, any] {
	if left == nil || right == nil {
		return New(name, func(_ context.Context, _ any) (any, error) {
			return nil, errors.New("pipe: left and right actions must not be nil")
		})
	}
	builder := New(name, func(ctx context.Context, request any) (any, error) {
		intermediate, err := InvokeAny(ctx, left, request)
		if err != nil {
			return nil, err
		}
		return InvokeAny(ctx, right, intermediate)
	}).Tag("pipe", "dynamic")

	builder.Route(left.GetBindings()...)
	builder.Route(right.GetBindings()...)
	return builder
}

func ParallelAny(name string, branches ...AnyAction) *Builder[any, map[string]any] {
	return New(name, func(ctx context.Context, request any) (map[string]any, error) {
		type resultSlot struct {
			key   string
			value any
			err   error
		}
		total := len(branches)
		ch := make(chan resultSlot, total)

		for i, branch := range branches {
			branchKey := fmt.Sprintf("branch_%d", i)
			if meta := branch.Describe(); meta != nil && meta.Name != "" {
				branchKey = meta.Name
			}
			go func(k string, a AnyAction) {
				defer func() {
					if r := recover(); r != nil {
						ch <- resultSlot{key: k, err: xerr.PanicRecovery(r)}
					}
				}()
				val, err := InvokeAny(ctx, a, request)
				ch <- resultSlot{key: k, value: val, err: err}
			}(branchKey, branch)
		}

		results := make(map[string]any, total)
		var firstErr error
		for range total {
			slot := <-ch
			results[slot.key] = slot.value
			if slot.err != nil && firstErr == nil {
				firstErr = slot.err
			}
		}
		return results, firstErr
	}).Tag("parallel", "dynamic")
}

func FirstSuccessAny(name string, candidates ...AnyAction) *Builder[any, any] {
	return New(name, func(ctx context.Context, request any) (any, error) {
		var lastErr error
		for _, candidate := range candidates {
			out, err := InvokeAny(ctx, candidate, request)
			if err == nil {
				return out, nil
			}
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		if lastErr == nil {
			lastErr = errors.New("no candidates provided")
		}
		return nil, fmt.Errorf("first_success [%s]: all failed: %w", name, lastErr)
	}).Tag("fallback", "dynamic")
}

func RoundRobinAny(name string, members ...AnyAction) *Builder[any, any] {
	total := uint64(len(members))
	var counter atomic.Uint64
	return New(name, func(ctx context.Context, request any) (any, error) {
		if total == 0 {
			return nil, errors.New("round_robin: pool has no members")
		}
		slot := (counter.Add(1) - 1) % total
		return InvokeAny(ctx, members[slot], request)
	}).Tag("pool", "round_robin")
}

func HashRouterAny(name string, keyFn func(any) string, members ...AnyAction) *Builder[any, any] {
	total := uint64(len(members))
	return New(name, func(ctx context.Context, request any) (any, error) {
		if total == 0 || keyFn == nil {
			return nil, errors.New("hash_router: invalid config")
		}
		hasher := fnv.New64a()
		_, _ = hasher.Write([]byte(keyFn(request)))
		return InvokeAny(ctx, members[hasher.Sum64()%total], request)
	}).Tag("pool", "hash")
}

func LoopAny(name string, body AnyAction, until func(any) bool, maxTurns int) *Builder[any, any] {
	if maxTurns <= 0 {
		maxTurns = 15
	}
	return New(name, func(ctx context.Context, input any) (any, error) {
		current := input
		for turn := 1; turn <= maxTurns; turn++ {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			out, err := InvokeAny(ctx, body, current)
			if err != nil {
				return nil, err
			}
			if until != nil && until(out) {
				return out, nil
			}
			current = out
		}
		return nil, xerr.Timeout(fmt.Sprintf("loop [%s]: exceeded %d turns", name, maxTurns))
	}).Tag("loop", "dynamic")
}

func AssertAny(name string, predicate func(any) bool, msg string) *Builder[any, any] {
	return New(name, func(_ context.Context, request any) (any, error) {
		if predicate != nil && !predicate(request) {
			if msg == "" {
				msg = fmt.Sprintf("assertion [%s] failed", name)
			}
			return nil, xerr.Validation(msg)
		}
		return request, nil
	}).Tag("guard", "assert")
}

// SwitchAny acts like an exact-match router on a state payload.
func SwitchAny(name string, selector func(any) string, cases map[string]AnyAction, defaultCase AnyAction) *Builder[any, any] {
	return New(name, func(ctx context.Context, request any) (any, error) {
		if selector == nil {
			return nil, errors.New("switch: selector required")
		}
		key := selector(request)
		target, exists := cases[key]
		if !exists {
			if defaultCase != nil {
				return InvokeAny(ctx, defaultCase, request)
			}
			return nil, fmt.Errorf("switch [%s]: unhandled case %q", name, key)
		}
		return InvokeAny(ctx, target, request)
	}).Tag("switch", "dynamic")
}

// CatchAny isolates errors and routes them to a recovery handler.
func CatchAny(name string, protected AnyAction, recoverFn func(context.Context, any, error) (any, error)) *Builder[any, any] {
	return New(name, func(ctx context.Context, request any) (any, error) {
		if protected == nil {
			return nil, errors.New("catch: protected action is nil")
		}
		res, err := InvokeAny(ctx, protected, request)
		if err != nil && recoverFn != nil {
			return recoverFn(ctx, request, err)
		}
		return res, err
	}).Tag("catch", "dynamic")
}

// RaceAny runs candidates concurrently. The first success wins and cancels
// the rest immediately, but RaceAny always waits for every goroutine to
// finish before returning — so none is left reading the caller's ctx
// (e.g. a pooled xctx.RequestScope) after the call has returned.
func RaceAny(name string, candidates ...AnyAction) *Builder[any, any] {
	total := len(candidates)
	return New(name, func(ctx context.Context, request any) (any, error) {
		if total == 0 {
			return nil, errors.New("race: no candidates provided")
		}

		raceCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		type outcome struct {
			value any
			err   error
		}
		// Buffer size == total guarantees every candidate goroutine can send
		// and terminate on its own, with no background drainer goroutine.
		results := make(chan outcome, total)

		for _, candidate := range candidates {
			go func(currentCandidate AnyAction) {
				defer func() {
					if recoveredPanic := recover(); recoveredPanic != nil {
						results <- outcome{err: xerr.PanicRecovery(recoveredPanic)}
					}
				}()
				val, err := InvokeAny(raceCtx, currentCandidate, request)
				results <- outcome{value: val, err: err}
			}(candidate)
		}

		var won bool
		var winnerValue any
		var lastErr error

		for range total {
			out := <-results
			if out.err == nil && !won {
				won = true
				winnerValue = out.value
				cancel() // ask remaining candidates to stop as early as possible
				continue
			}
			if out.err != nil {
				lastErr = out.err
			}
		}

		if won {
			return winnerValue, nil
		}
		return nil, fmt.Errorf("race [%s] all candidates failed; last error: %w", name, lastErr)
	}).Tag("race", "dynamic")
}

type DynamicSagaStep struct {
	Name     string
	Do       AnyAction
	Undo     AnyAction
	Optional bool
}

// SagaAny guarantees LIFO execution of compensations if a downstream step fails.
func SagaAny(name string, steps []DynamicSagaStep) *Builder[any, any] {
	return New(name, func(ctx context.Context, initialPayload any) (any, error) {
		completed := make([]int, 0, len(steps))
		currentPayload := initialPayload

		for i, step := range steps {
			if step.Do == nil {
				return nil, fmt.Errorf("saga [%s] step %q lacks Do action", name, step.Name)
			}
			out, err := InvokeAny(ctx, step.Do, currentPayload)
			if err != nil {
				if step.Optional {
					continue
				}
				rollbackCtx := context.WithoutCancel(ctx)
				var compensationErrors []error
				for _, c := range slices.Backward(completed) {
					undoAct := steps[c].Undo
					if undoAct != nil {
						if _, undoErr := InvokeAny(rollbackCtx, undoAct, initialPayload); undoErr != nil {
							compensationErrors = append(compensationErrors, fmt.Errorf("step %q: %w", steps[c].Name, undoErr))
							slog.Error("saga_undo_failed", "saga", name, "step", steps[c].Name, "error", undoErr)
						}
					}
				}
				primaryErr := fmt.Errorf("saga [%s] failed at %q: %w", name, step.Name, err)
				if len(compensationErrors) > 0 {
					return nil, errors.Join(primaryErr, fmt.Errorf("saga [%s] compensation failures: %w", name, errors.Join(compensationErrors...)))
				}
				return nil, primaryErr
			}
			completed = append(completed, i)
			currentPayload = out
		}
		return currentPayload, nil
	}).Tag("saga", "dynamic")
}

type (
	StateExtractor func(any) string
	StateSetter    func(any, string)
)

// StateMachineAny enforces state machine transition matrices dynamically.
func StateMachineAny(name string, get StateExtractor, set StateSetter, transitions map[string][]string, inner AnyAction) *Builder[any, any] {
	return New(name, func(ctx context.Context, request any) (any, error) {
		origState := get(request)
		res, err := InvokeAny(ctx, inner, request)
		if err != nil {
			return nil, err
		}
		newState := get(res)
		if origState == newState {
			return res, nil
		}
		allowed, exists := transitions[origState]
		if !exists {
			set(request, origState)
			return nil, xerr.Conflict(fmt.Sprintf("statemachine: invalid start state %q", origState))
		}
		valid := slices.Contains(allowed, newState)
		if !valid {
			set(request, origState)
			return nil, xerr.Conflict(fmt.Sprintf("statemachine: transition %q -> %q rejected", origState, newState))
		}
		return res, nil
	}).Tag("state_machine", "dynamic")
}

// TapAny executes an inspector side-effect (e.g. audit) and returns the request unchanged.
func TapAny(name string, inspector func(context.Context, any) error) *Builder[any, any] {
	return New(name, func(ctx context.Context, request any) (any, error) {
		if inspector != nil {
			if err := inspector(ctx, request); err != nil {
				return nil, fmt.Errorf("tap [%s]: %w", name, err)
			}
		}
		return request, nil
	}).Tag("tap", "dynamic")
}

// TernaryAny executes cond; if truthy, executes thenBranch, else executes elseBranch.
// elseBranch may be nil for a two-arm conditional (skip if false).
func TernaryAny(name string, cond, thenBranch, elseBranch AnyAction) *Builder[any, any] {
	return New(name, func(ctx context.Context, request any) (any, error) {
		gate, err := InvokeAny(ctx, cond, request)
		if err != nil {
			return nil, err
		}
		if isTruthyAny(gate) {
			return InvokeAny(ctx, thenBranch, request)
		}
		if elseBranch == nil {
			return nil, nil
		}
		return InvokeAny(ctx, elseBranch, request)
	}).Tag("conditional", "dynamic")
}

// isTruthyAny determines boolean truthiness for dynamic routing.
func isTruthyAny(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case int:
		return x != 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	case string:
		return x != "" && x != "false" && x != "no" && x != "0"
	case map[string]any:
		for _, val := range x {
			if isTruthyAny(val) {
				return true
			}
		}
		return false
	}
	return true
}
