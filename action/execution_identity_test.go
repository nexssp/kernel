package action

import (
	"errors"
	"testing"

	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
)

func TestDigestBytesAndStringAgree(t *testing.T) {
	data := []byte("nexss execution")
	fromBytes := DigestBytes(data)
	fromString := DigestString(string(data))

	if !fromBytes.Equal(fromString) {
		t.Fatal("DigestBytes and DigestString returned different digests")
	}
	if fromBytes.IsZero() {
		t.Fatal("digest unexpectedly has the zero value")
	}
	if DigestBytes(nil) != DigestBytes([]byte{}) {
		t.Fatal("nil and empty input must have the same digest")
	}
}

func TestExecutionIdentityBinding(t *testing.T) {
	base := ExecutionIdentity{
		ExecutionID:     "exec-1",
		RootExecutionID: "exec-1",
		SourceDigest:    DigestString("source-v1"),
		InputDigest:     DigestString("input-v1"),
		ContractDigest:  DigestString(ExecutionContractVersion),
	}

	if err := base.Validate(); err != nil {
		t.Fatalf("base identity should validate: %v", err)
	}
	if !base.SameExecution(base) || !base.BindingEqual(base) {
		t.Fatal("identity should match itself")
	}

	sameExecutionDifferentRequest := base
	if !sameExecutionDifferentRequest.SameExecution(base) {
		t.Fatal("request identity is intentionally outside ExecutionIdentity")
	}

	differentSource := base
	differentSource.SourceDigest = DigestString("source-v2")
	if !differentSource.SameExecution(base) {
		t.Fatal("source change must not change the execution namespace")
	}
	if differentSource.BindingEqual(base) {
		t.Fatal("source change must invalidate the durable binding")
	}
	if !errors.Is(differentSource.ValidateBinding(base), ErrSourceDigestMismatch) {
		t.Fatal("source change should return ErrSourceDigestMismatch")
	}
	if got := xerr.KindFrom(differentSource.ValidateBinding(base)); got != xerr.KindConflict {
		t.Fatalf("source change kind = %s, want Conflict", got)
	}

	differentInput := base
	differentInput.InputDigest = DigestString("input-v2")
	if differentInput.BindingEqual(base) {
		t.Fatal("input change must invalidate the durable binding")
	}
	if !errors.Is(differentInput.ValidateBinding(base), ErrInputDigestMismatch) {
		t.Fatal("input change should return ErrInputDigestMismatch")
	}

	differentContract := base
	differentContract.ContractDigest = DigestString("nexss.execution-contract/v2")
	if differentContract.BindingEqual(base) {
		t.Fatal("contract change must invalidate the durable binding")
	}
	if !errors.Is(differentContract.ValidateBinding(base), ErrContractDigestMismatch) {
		t.Fatal("contract change should return ErrContractDigestMismatch")
	}

	differentGeneration := base
	differentGeneration.Generation = 1
	differentGeneration.ParentExecutionID = base.ExecutionID
	if differentGeneration.SameExecution(base) {
		t.Fatal("new generation must create a new execution namespace")
	}
	if differentGeneration.BindingEqual(base) {
		t.Fatal("new generation must not reuse the old binding")
	}
	if !errors.Is(differentGeneration.ValidateBinding(base), ErrExecutionNamespaceMismatch) {
		t.Fatal("generation change should return ErrExecutionNamespaceMismatch")
	}
	if err := differentGeneration.Validate(); err != nil {
		t.Fatalf("forced-resume generation should validate: %v", err)
	}
}

func TestExecutionIdentityValidation(t *testing.T) {
	cases := []struct {
		name string
		id   ExecutionIdentity
	}{
		{name: "missing execution ID"},
		{
			name: "generation without parent",
			id: ExecutionIdentity{
				ExecutionID: "exec-1",
				Generation:  1,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.id.Validate(); err == nil {
				t.Fatal("Validate returned nil for invalid identity")
			}
		})
	}

	if !errors.Is(cases[0].id.Validate(), ErrExecutionIDRequired) {
		t.Fatal("missing execution ID should return ErrExecutionIDRequired")
	}
	if got := xerr.KindFrom(cases[0].id.Validate()); got != xerr.KindValidation {
		t.Fatalf("missing execution ID kind = %s, want Validation", got)
	}
	if !errors.Is(cases[1].id.Validate(), ErrParentExecutionIDRequired) {
		t.Fatal("missing parent should return ErrParentExecutionIDRequired")
	}
}

func TestDigestHotPathZeroAlloc(t *testing.T) {
	digest := DigestString("hot-path")
	other := DigestString("other")

	allocs := testing.AllocsPerRun(1000, func() {
		if digest.Equal(other) {
			t.Fatal("unexpected equal digest")
		}
		if digest.IsZero() {
			t.Fatal("unexpected zero digest")
		}
	})
	if allocs != 0 {
		t.Fatalf("Digest equality/zero checks allocated %.2f times per call, want 0", allocs)
	}
}

func TestExecutionIdentityComparisonZeroAlloc(t *testing.T) {
	id := ExecutionIdentity{
		ExecutionID:    "exec-hot-path",
		Generation:     3,
		SourceDigest:   DigestString("source"),
		InputDigest:    DigestString("input"),
		ContractDigest: DigestString(ExecutionContractVersion),
	}
	other := id

	allocs := testing.AllocsPerRun(1000, func() {
		if !id.SameExecution(other) {
			t.Fatal("identities should share an execution namespace")
		}
		if !id.BindingEqual(other) {
			t.Fatal("identities should have the same binding")
		}
	})
	if allocs != 0 {
		t.Fatalf("ExecutionIdentity comparisons allocated %.2f times per call, want 0", allocs)
	}
}

func TestExecutionIdentityValidationSuccessZeroAlloc(t *testing.T) {
	id := ExecutionIdentity{
		ExecutionID:    "exec-valid",
		SourceDigest:   DigestString("source"),
		InputDigest:    DigestString("input"),
		ContractDigest: DigestString(ExecutionContractVersion),
	}
	xtest.RequireZeroAlloc(t, 1000, func() {
		if err := id.Validate(); err != nil {
			t.Fatalf("valid identity returned an error: %v", err)
		}
		if err := id.ValidateBinding(id); err != nil {
			t.Fatalf("matching binding returned an error: %v", err)
		}
	})
}

func TestDigestCreationDoesNotAllocate(t *testing.T) {
	data := []byte("digest-input")
	value := "digest-input"
	allocs := testing.AllocsPerRun(1000, func() {
		_ = DigestBytes(data)
		_ = DigestString(value)
	})
	if allocs != 0 {
		t.Fatalf("DigestBytes/DigestString allocated %.2f times per call, want 0", allocs)
	}
}
