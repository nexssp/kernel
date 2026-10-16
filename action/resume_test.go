package action_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
)

func resumeIdentity() action.ExecutionIdentity {
	return action.ExecutionIdentity{
		ExecutionID:     "exec-1",
		RootExecutionID: "exec-1",
		SourceDigest:    action.DigestString("source-v1"),
		InputDigest:     action.DigestString("input-v1"),
		ContractDigest:  action.DigestString(action.ExecutionContractVersion),
	}
}

func TestValidateResume_AllowsMatchingBinding(t *testing.T) {
	identity := resumeIdentity()
	if err := action.ValidateResume(identity, identity, action.ResumeOptions{}); err != nil {
		t.Fatalf("matching binding: %v", err)
	}
}

func TestValidateResume_RejectsMismatchWithoutForce(t *testing.T) {
	existing := resumeIdentity()
	requested := existing
	requested.InputDigest = action.DigestString("input-v2")

	err := action.ValidateResume(existing, requested, action.ResumeOptions{})
	if !errors.Is(err, action.ErrForceResumeRequired) {
		t.Fatalf("expected ErrForceResumeRequired, got %v", err)
	}
	if xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("kind = %s, want Conflict", xerr.KindFrom(err))
	}
}

func TestValidateResume_RequiresReason(t *testing.T) {
	existing := resumeIdentity()
	requested := existing
	requested.Generation = 1
	requested.ParentExecutionID = existing.ExecutionID
	requested.InputDigest = action.DigestString("input-v2")

	err := action.ValidateResume(existing, requested, action.ResumeOptions{Force: true})
	if !errors.Is(err, action.ErrForceReasonRequired) {
		t.Fatalf("expected ErrForceReasonRequired, got %v", err)
	}
	if xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("kind = %s, want Validation", xerr.KindFrom(err))
	}
}

func TestValidateResume_AllowsSuccessorGeneration(t *testing.T) {
	existing := resumeIdentity()
	requested := existing
	requested.Generation = 1
	requested.ParentExecutionID = existing.ExecutionID
	requested.InputDigest = action.DigestString("input-v2")

	err := action.ValidateResume(existing, requested, action.ResumeOptions{
		Force:       true,
		ForceReason: "source and input intentionally changed",
	})
	if err != nil {
		t.Fatalf("successor validation: %v", err)
	}
}

func TestValidateResume_RejectsInvalidSuccessor(t *testing.T) {
	existing := resumeIdentity()
	requested := existing
	requested.InputDigest = action.DigestString("input-v2")

	err := action.ValidateResume(existing, requested, action.ResumeOptions{
		Force:       true,
		ForceReason: "explicit test force",
	})
	if !errors.Is(err, action.ErrInvalidSuccessor) {
		t.Fatalf("expected ErrInvalidSuccessor, got %v", err)
	}
	if xerr.KindFrom(err) != xerr.KindValidation {
		t.Fatalf("kind = %s, want Validation", xerr.KindFrom(err))
	}
}

func TestValidateResume_MatchingBindingZeroAlloc(t *testing.T) {
	identity := resumeIdentity()
	xtest.RequireZeroAlloc(t, 1000, func() {
		if err := action.ValidateResume(identity, identity, action.ResumeOptions{}); err != nil {
			t.Fatalf("matching binding: %v", err)
		}
	})
}

func TestResumeJournalContract(t *testing.T) {
	var journal action.ResumeJournal = resumeJournalStub{}
	if _, _, err := journal.GetStep(context.Background(), "", ""); err != nil {
		t.Fatal(err)
	}
}

func TestDurableWithResume_ValidatesFingerprintBeforeReplay(t *testing.T) {
	journal := newResumeMemoryJournal()
	var calls atomic.Int32
	identityFn := func(_ context.Context, input string) (action.ExecutionIdentity, error) {
		identity := resumeIdentity()
		identity.InputDigest = action.DigestString(input)
		return identity, nil
	}
	step := action.New("resume-step", func(_ context.Context, input string) (string, error) {
		calls.Add(1)
		return input + ":result", nil
	}).Use(action.DurableWithResume[string, string](journal, "step", identityFn, action.ResumeOptions{})).Build()

	if got, err := step.Do(context.Background(), "input-v1"); err != nil || got != "input-v1:result" {
		t.Fatalf("first call: %q / %v", got, err)
	}
	if got, err := step.Do(context.Background(), "input-v1"); err != nil || got != "input-v1:result" {
		t.Fatalf("replay: %q / %v", got, err)
	}
	if _, err := step.Do(context.Background(), "input-v2"); xerr.KindFrom(err) != xerr.KindConflict {
		t.Fatalf("changed input kind = %s, want Conflict", xerr.KindFrom(err))
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}
}

type resumeJournalStub struct{}

func TestDurableWithResume_ForcedSuccessorUsesNewGeneration(t *testing.T) {
	journal := newResumeMemoryJournal()
	var calls atomic.Int32
	identityFn := func(_ context.Context, input string) (action.ExecutionIdentity, error) {
		identity := resumeIdentity()
		identity.InputDigest = action.DigestString(input)
		if input == "input-v2" {
			identity.Generation = 1
			identity.ParentExecutionID = identity.ExecutionID
		}
		return identity, nil
	}
	step := action.New("resume-successor", func(_ context.Context, input string) (string, error) {
		calls.Add(1)
		return input, nil
	}).Use(action.DurableWithResume[string, string](journal, "step", identityFn, action.ResumeOptions{
		Force:       true,
		ForceReason: "intentional input migration",
	})).Build()

	if _, err := step.Do(context.Background(), "input-v1"); err != nil {
		t.Fatal(err)
	}
	if got, err := step.Do(context.Background(), "input-v2"); err != nil || got != "input-v2" {
		t.Fatalf("successor: %q / %v", got, err)
	}
	if got, err := step.Do(context.Background(), "input-v2"); err != nil || got != "input-v2" {
		t.Fatalf("successor replay: %q / %v", got, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls = %d, want 2", calls.Load())
	}
}

func (resumeJournalStub) GetStep(context.Context, string, string) (payload []byte, found bool, err error) {
	return
}
func (resumeJournalStub) RecordStep(context.Context, string, string, []byte) error { return nil }
func (resumeJournalStub) GetExecutionIdentity(context.Context, string) (action.ExecutionIdentity, bool, error) {
	return action.ExecutionIdentity{}, false, nil
}

func (resumeJournalStub) RecordExecutionIdentity(context.Context, action.ExecutionIdentity) error {
	return nil
}

type resumeMemoryJournal struct {
	mu         sync.Mutex
	identities map[string]action.ExecutionIdentity
	records    map[string][]byte
}

func newResumeMemoryJournal() *resumeMemoryJournal {
	return &resumeMemoryJournal{
		identities: make(map[string]action.ExecutionIdentity),
		records:    make(map[string][]byte),
	}
}

func (m *resumeMemoryJournal) GetStep(_ context.Context, executionID, stepName string) (payload []byte, found bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	payload, found = m.records[executionID+"\x00"+stepName]
	return
}

func (m *resumeMemoryJournal) RecordStep(_ context.Context, executionID, stepName string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[executionID+"\x00"+stepName] = append([]byte(nil), payload...)
	return nil
}

func (m *resumeMemoryJournal) GetExecutionIdentity(_ context.Context, executionID string) (action.ExecutionIdentity, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.identities[executionID]
	return id, ok, nil
}

func (m *resumeMemoryJournal) RecordExecutionIdentity(_ context.Context, identity action.ExecutionIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.identities[identity.ExecutionID] = identity
	return nil
}
