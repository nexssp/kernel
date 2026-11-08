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
	return ParallelAnyWithOptions(name, AsyncOptions{}, branches...)
}

// ParallelAnyWithOptions executes dynamic branches with configurable limits
// and joins all started callbacks before returning.
func ParallelAnyWithOptions(name string, options AsyncOptions, branches ...AnyAction) *Builder[any, map[string]any] {
	return New(name, func(ctx context.Context, request any) (map[string]any, error) {
		total := len(branches)
		keys := make([]string, total)
		describeErrors := make([]error, total)
		for i, branch := range branches {
			keys[i] = fmt.Sprintf("branch_%d", i)
			if branch != nil {
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							describeErrors[i] = xerr.PanicRecovery(recovered)
						}
					}()
					if meta := branch.Describe(); meta != nil && meta.Name != "" {
						keys[i] = meta.Name
					}
				}()
			}
		}
		var completionCounter atomic.Uint64
		completedAt := make([]uint64, total)
		outcomes := runAsyncTasks(ctx, total, options, func(ctx context.Context, index int) (any, error) {
			var value any
			var err error
			defer func() { completedAt[index] = completionCounter.Add(1) }()
			if describeErrors[index] != nil {
				return nil, describeErrors[index]
			}
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						err = xerr.PanicRecovery(recovered)
					}
				}()
				value, err = InvokeAny(ctx, branches[index], request)
			}()
			return value, err
		})
		results := make(map[string]any, total)
		resultCompletion := make(map[string]uint64, total)
		var firstErr error
		firstErrorCompletion := ^uint64(0)
		for i, outcome := range outcomes {
			if previous, seen := resultCompletion[keys[i]]; !seen || completedAt[i] >= previous {
				results[keys[i]] = outcome.Value
				resultCompletion[keys[i]] = completedAt[i]
			}
			if outcome.Err != nil && completedAt[i] < firstErrorCompletion {
				firstErr = outcome.Err
				firstErrorCompletion = completedAt[i]
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
			message := msg
			if message == "" {
				message = fmt.Sprintf("assertion [%s] failed", name)
			}
			return nil, xerr.Validation(message)
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

// RaceAny runs up to DefaultAsyncMaxConcurrency candidates at a time, with
// DefaultAsyncTimeout. The first success wins and cancels the rest, but RaceAny
// joins all started candidates before returning so none is left reading a
// caller-owned request scope.
func RaceAny(name string, candidates ...AnyAction) *Builder[any, any] {
	return RaceAnyWithOptions(name, AsyncOptions{}, candidates...)
}

type raceAnyOutcome struct {
	value any
	err   error
}

func launchRaceAnyCandidate(ctx context.Context, request any, candidate AnyAction, results chan<- raceAnyOutcome) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				results <- raceAnyOutcome{err: xerr.PanicRecovery(recovered)}
			}
		}()
		value, err := InvokeAny(ctx, candidate, request)
		results <- raceAnyOutcome{value: value, err: err}
	}()
}

// RaceAnyWithOptions runs dynamic candidates with configurable concurrency and
// an overall timeout. It joins all started candidates before returning; a
// callback that ignores context can make the call exceed its deadline.
func RaceAnyWithOptions(name string, options AsyncOptions, candidates ...AnyAction) *Builder[any, any] {
	options = options.withDefaults()
	total := len(candidates)
	return New(name, func(ctx context.Context, request any) (any, error) {
		if total == 0 {
			return nil, errors.New("race: no candidates provided")
		}

		raceCtx, cancel := context.WithTimeout(ctx, options.Timeout)
		defer cancel()

		limit := min(options.MaxConcurrency, total)
		results := make(chan raceAnyOutcome, limit)

		var won bool
		var winnerValue any
		var lastErr error
		next, active := 0, 0
		var terminalErr error
		done := raceCtx.Done()
		for next < total || active > 0 {
			for !won && terminalErr == nil && next < total && active < limit && raceCtx.Err() == nil {
				launchRaceAnyCandidate(raceCtx, request, candidates[next], results)
				next++
				active++
			}
			if active == 0 {
				break
			}
			select {
			case out := <-results:
				active--
				if terminalErr == nil && raceCtx.Err() == nil && out.err == nil && !won {
					won = true
					winnerValue = out.value
					cancel()
					done = nil
					continue
				}
				if out.err != nil {
					lastErr = out.err
				}
			case <-done:
				terminalErr = raceCtx.Err()
				cancel()
				done = nil
			}
		}

		if won {
			return winnerValue, nil
		}
		if terminalErr == nil {
			terminalErr = raceCtx.Err()
		}
		if terminalErr != nil {
			return nil, terminalErr
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
	return SagaAnyWithOptions(name, steps, SagaOptions{})
}

// SagaAnyWithOptions executes a dynamic Saga with the configured compensation
// timeout applied independently to each undo step.
func SagaAnyWithOptions(name string, steps []DynamicSagaStep, options SagaOptions) *Builder[any, any] {
	options = options.withDefaults()
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
						undoCtx, cancelUndo := context.WithTimeout(rollbackCtx, options.CompensationTimeout)
						_, undoErr := InvokeAny(undoCtx, undoAct, initialPayload)
						cancelUndo()
						if undoErr != nil {
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
	transitions = cloneStateTransitions(transitions)
	return New(name, func(ctx context.Context, request any) (any, error) {
		origState := get(request)
		allowed, exists := transitions[origState]
		if !exists {
			return nil, xerr.Conflict(fmt.Sprintf("statemachine: invalid start state %q", origState))
		}
		res, err := InvokeAny(ctx, inner, request)
		if err != nil {
			set(request, origState)
			return nil, err
		}
		newState := get(res)
		if origState == newState {
			return res, nil
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
