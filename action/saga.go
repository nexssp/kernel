package action

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexssp/kernel/xerr"
)

// StepResult captures execution metadata for a single Saga step.
type StepResult struct {
	Step       string `json:"step"`
	Success    bool   `json:"success"`
	Skipped    bool   `json:"skipped,omitempty"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

// SagaResult captures the full execution audit and output of the Saga.
type SagaResult[Res any] struct {
	Saga       string       `json:"saga"`
	Success    bool         `json:"success"`
	Output     Res          `json:"output,omitempty"`
	Steps      []StepResult `json:"steps"`
	Error      string       `json:"error,omitempty"`
	RolledBack bool         `json:"rolled_back,omitempty"`
	DurationMs int64        `json:"duration_ms"`
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
func (s *BuiltSaga[Req, Res]) Do(ctx context.Context, req Req) (SagaResult[Res], error) {
	start := time.Now()
	stepResults := make([]StepResult, 0, len(s.steps))
	var lastRes Res

	for i, step := range s.steps {
		stepStart := time.Now()
		res, err := executeSagaDo(ctx, step, req)
		dur := time.Since(stepStart).Milliseconds()

		if err != nil {
			if step.Optional {
				stepResults = append(stepResults, StepResult{
					Step:       step.Name,
					Success:    false,
					Skipped:    true,
					DurationMs: dur,
				})
				continue
			}

			stepResults = append(stepResults, StepResult{
				Step:       step.Name,
				Success:    false,
				Error:      err.Error(),
				DurationMs: dur,
			})

			rollbackCtx := context.WithoutCancel(ctx)
			for j := i - 1; j >= 0; j-- {
				if s.steps[j].Undo != nil && !stepResults[j].Skipped {
					if undoErr := executeSagaUndo(rollbackCtx, s.steps[j], req); undoErr != nil {
						slog.Error("saga_undo_failed", "saga", s.name, "step", s.steps[j].Name, "error", undoErr)
					}
				}
			}

			var zero Res
			return SagaResult[Res]{
				Saga:       s.name,
				Success:    false,
				Steps:      stepResults,
				Error:      fmt.Sprintf("saga [%s] failed at step [%s]: %v", s.name, step.Name, err),
				RolledBack: true,
				DurationMs: time.Since(start).Milliseconds(),
				Output:     zero,
			}, fmt.Errorf("saga [%s] failed at step [%s]: %w", s.name, step.Name, err)
		}

		stepResults = append(stepResults, StepResult{
			Step:       step.Name,
			Success:    true,
			DurationMs: dur,
		})
		lastRes = res
	}

	return SagaResult[Res]{
		Saga:       s.name,
		Success:    true,
		Output:     lastRes,
		Steps:      stepResults,
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
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
