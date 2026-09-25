// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.

package action

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// MaxSagaSteps caps the inline step-result storage. Sagas with more than
// MaxSagaSteps steps fall back to heap allocation. 32 is large enough for
// virtually all real-world distributed transactions while keeping the
// SagaResult struct a fixed-size, zero-alloc return value.
const MaxSagaSteps = 32

// StepResult captures execution metadata for a single Saga step.
type StepResult struct {
	Step       string `json:"step"`
	Success    bool   `json:"success"`
	Skipped    bool   `json:"skipped,omitempty"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

// SagaResult captures the full execution audit and output of the Saga.
//
// Steps is a fixed-size array (not a slice) so the return value is a single,
// zero-allocation value copy. StepsCount indicates how many entries in
// Steps are populated; callers iterate Steps[:StepsCount].
//
// For sagas larger than MaxSagaSteps, Steps overflows to a heap-allocated
// slice stored in StepsOverflow. This is the rare case — most sagas have
// fewer than 32 steps.
type SagaResult[Res any] struct {
	Saga          string                   `json:"saga"`
	Success       bool                     `json:"success"`
	Output        Res                      `json:"output,omitempty"`
	Steps         [MaxSagaSteps]StepResult `json:"steps"`
	StepsCount    int                      `json:"steps_count"`
	StepsOverflow []StepResult             `json:"steps_overflow,omitempty"`
	Error         string                   `json:"error,omitempty"`
	RolledBack    bool                     `json:"rolled_back,omitempty"`
	DurationMs    int64                    `json:"duration_ms"`
}

// Step returns the i-th step result, abstracting the inline/overflow split.
func (r *SagaResult[Res]) Step(i int) StepResult {
	if i < MaxSagaSteps {
		return r.Steps[i]
	}
	return r.StepsOverflow[i-MaxSagaSteps]
}

// AllSteps returns a slice view over all populated step results.
// Allocates only if the saga overflowed the inline array.
func (r *SagaResult[Res]) AllSteps() []StepResult {
	if r.StepsCount <= MaxSagaSteps {
		return r.Steps[:r.StepsCount]
	}
	all := make([]StepResult, 0, r.StepsCount)
	all = append(all, r.Steps[:]...)
	all = append(all, r.StepsOverflow...)
	return all
}

// SagaStep represents a single operation and its compensating rollback.
type SagaStep[Req, Res any] struct {
	Name     string
	Do       func(context.Context, Req) (Res, error)
	Undo     func(context.Context, Req) error
	Optional bool
}

// SagaBuilder constructs a distributed transaction pipeline.
type SagaBuilder[Req, Res any] struct {
	name  string
	steps []SagaStep[Req, Res]
}

// NewSaga initializes an in-memory Saga pipeline.
func NewSaga[Req, Res any](name string) *SagaBuilder[Req, Res] {
	return &SagaBuilder[Req, Res]{name: name}
}

// AddStep appends a mandatory (Do, Undo) pair.
func (s *SagaBuilder[Req, Res]) AddStep(
	name string,
	do func(context.Context, Req) (Res, error),
	undo func(context.Context, Req) error,
) *SagaBuilder[Req, Res] {
	s.steps = append(s.steps, SagaStep[Req, Res]{Name: name, Do: do, Undo: undo, Optional: false})
	return s
}

// AddOptionalStep appends a step that skips on failure without triggering a Saga rollback.
func (s *SagaBuilder[Req, Res]) AddOptionalStep(
	name string,
	do func(context.Context, Req) (Res, error),
	undo func(context.Context, Req) error,
) *SagaBuilder[Req, Res] {
	s.steps = append(s.steps, SagaStep[Req, Res]{Name: name, Do: do, Undo: undo, Optional: true})
	return s
}

// BuiltSaga is the immutable, executable Saga.
type BuiltSaga[Req, Res any] struct {
	name  string
	steps []SagaStep[Req, Res]
}

// Build compiles the Saga.
func (s *SagaBuilder[Req, Res]) Build() *BuiltSaga[Req, Res] {
	return &BuiltSaga[Req, Res]{name: s.name, steps: s.steps}
}

// Do executes the Saga. If a mandatory step fails, it automatically runs Undo functions in reverse order.
//
// Hot-path: zero heap allocations for sagas with ≤ MaxSagaSteps steps.
// Step results are written into the inline [MaxSagaSteps]StepResult array
// on the stack/return-value. Only sagas with more than MaxSagaSteps steps
// fall back to heap allocation via StepsOverflow.
func (s *BuiltSaga[Req, Res]) Do(ctx context.Context, req Req) (SagaResult[Res], error) {
	start := time.Now()
	var result SagaResult[Res]
	result.Saga = s.name
	// Inline step result write — no slice allocation.
	stepResults := &result.Steps // [MaxSagaSteps]StepResult
	stepCount := 0               // tracks StepsCount inline
	var lastRes Res

	appendStep := func(sr StepResult) {
		if stepCount < MaxSagaSteps {
			stepResults[stepCount] = sr
		} else {
			// Overflow: first overflow allocates the slice.
			if result.StepsOverflow == nil {
				result.StepsOverflow = make([]StepResult, 0, len(s.steps)-MaxSagaSteps)
			}
			result.StepsOverflow = append(result.StepsOverflow, sr)
		}
		stepCount++
	}

	stepAt := func(i int) *StepResult {
		if i < MaxSagaSteps {
			return &stepResults[i]
		}
		return &result.StepsOverflow[i-MaxSagaSteps]
	}

	for i, step := range s.steps {
		stepStart := time.Now()
		res, err := executeSagaDo(ctx, step, req)
		dur := time.Since(stepStart).Milliseconds()

		if err != nil {
			if step.Optional {
				appendStep(StepResult{
					Step:       step.Name,
					Success:    false,
					Skipped:    true,
					DurationMs: dur,
				})
				continue
			}

			appendStep(StepResult{
				Step:       step.Name,
				Success:    false,
				Error:      err.Error(),
				DurationMs: dur,
			})

			rollbackCtx := context.WithoutCancel(ctx)
			for j := i - 1; j >= 0; j-- {
				if s.steps[j].Undo != nil && !stepAt(j).Skipped {
					if undoErr := executeSagaUndo(rollbackCtx, s.steps[j], req); undoErr != nil {
						slog.Error("saga_undo_failed", "saga", s.name, "step", s.steps[j].Name, "error", undoErr)
					}
				}
			}

			var zero Res
			result.Success = false
			result.StepsCount = stepCount
			result.Error = fmt.Sprintf("saga [%s] failed at step [%s]: %v", s.name, step.Name, err)
			result.RolledBack = true
			result.DurationMs = time.Since(start).Milliseconds()
			result.Output = zero
			return result, fmt.Errorf("saga [%s] failed at step [%s]: %w", s.name, step.Name, err)
		}

		appendStep(StepResult{
			Step:       step.Name,
			Success:    true,
			DurationMs: dur,
		})
		lastRes = res
	}

	result.Success = true
	result.Output = lastRes
	result.StepsCount = stepCount
	result.DurationMs = time.Since(start).Milliseconds()
	return result, nil
}

func executeSagaDo[Req, Res any](ctx context.Context, step SagaStep[Req, Res], req Req) (res Res, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
		}
	}()
	return step.Do(ctx, req)
}

func executeSagaUndo[Req, Res any](ctx context.Context, step SagaStep[Req, Res], req Req) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = xerr.PanicRecovery(r)
		}
	}()
	return step.Undo(ctx, req)
}

// AsAction converts the compiled Saga into a standard *BuiltAction, allowing
// it to be routed over HTTP, CLI, MCP, or composed via action.Pipe / action.Parallel.
func (s *BuiltSaga[Req, Res]) AsAction() *BuiltAction[Req, SagaResult[Res]] {
	return New(s.name, s.Do).
		Description(fmt.Sprintf("Distributed Saga workflow: %s (%d steps)", s.name, len(s.steps))).
		Tag("saga", "workflow").
		Build()
}
