package action_test

import (
	"context"
	"iter"
	"reflect"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestStreamActionMetadataCopiesAndCloneOwnership(t *testing.T) {
	t.Parallel()

	source := action.NewStream("ownership.stream", func(context.Context, struct{}) (iter.Seq2[int, error], error) {
		return nil, nil
	})
	source.AddAnyHook(action.AnyHook{OnBuild: func(meta *action.Meta, _, _ reflect.Type) bool {
		meta.Tags = []string{"source"}
		meta.RequiredRoles = []string{"source-role"}
		meta.RequiredPermissions = []string{"source-permission"}
		meta.RequiredFeatures = []string{"source-feature"}
		return true
	}})

	assertMeta := func(meta *action.Meta, tag, role, permission, feature string) {
		t.Helper()
		if meta == nil {
			t.Fatal("metadata is nil")
		}
		if meta.Tags[0] != tag || meta.RequiredRoles[0] != role || meta.RequiredPermissions[0] != permission || meta.RequiredFeatures[0] != feature {
			t.Fatalf("metadata slices = (%v, %v, %v, %v), want (%q, %q, %q, %q)", meta.Tags, meta.RequiredRoles, meta.RequiredPermissions, meta.RequiredFeatures, tag, role, permission, feature)
		}
	}

	for _, meta := range []*action.Meta{source.GetMeta(), source.Describe()} {
		meta.Tags[0] = "accessor-tag"
		meta.RequiredRoles[0] = "accessor-role"
		meta.RequiredPermissions[0] = "accessor-permission"
		meta.RequiredFeatures[0] = "accessor-feature"
	}
	assertMeta(source.Describe(), "source", "source-role", "source-permission", "source-feature")

	clone := source.CloneWithHooks()
	clone.AddAnyHook(action.AnyHook{OnBuild: func(meta *action.Meta, _, _ reflect.Type) bool {
		meta.Tags[0] = "clone"
		meta.RequiredRoles[0] = "clone-role"
		meta.RequiredPermissions[0] = "clone-permission"
		meta.RequiredFeatures[0] = "clone-feature"
		return true
	}})
	assertMeta(clone.Describe(), "clone", "clone-role", "clone-permission", "clone-feature")
	assertMeta(source.Describe(), "source", "source-role", "source-permission", "source-feature")

	meta := clone.Describe()
	meta.Tags[0] = "accessor-tag"
	meta.RequiredRoles[0] = "accessor-role"
	meta.RequiredPermissions[0] = "accessor-permission"
	meta.RequiredFeatures[0] = "accessor-feature"
	assertMeta(clone.Describe(), "clone", "clone-role", "clone-permission", "clone-feature")
}

func TestStreamActionAccessorsAreNilSafe(t *testing.T) {
	t.Parallel()

	var stream *action.StreamAction[struct{}, int]
	if stream.GetMeta() != nil || stream.Describe() != nil || stream.GetBindings() != nil || stream.GetAnyHooks() != nil {
		t.Fatal("nil StreamAction accessors must return nil")
	}
	if stream.CloneWithHooks() != nil {
		t.Fatal("CloneWithHooks on a nil StreamAction must return nil")
	}
}

func TestBuiltActionCloneWithHooksNilReceiverReturnsNil(t *testing.T) {
	t.Parallel()

	var act *action.BuiltAction[int, int]
	if act.CloneWithHooks() != nil {
		t.Fatal("CloneWithHooks on a nil BuiltAction must return nil")
	}
}

func TestSagaResultAllStepsReturnsIndependentCopy(t *testing.T) {
	t.Parallel()

	inline := action.SagaResult[int]{StepsCount: 2}
	inline.Steps[0].Step = "first"
	inline.Steps[1].Step = "second"
	inlineSteps := inline.AllSteps()
	inlineSteps[0].Step = "changed-copy"
	if inline.Steps[0].Step != "first" {
		t.Fatalf("inline result changed through AllSteps copy: %q", inline.Steps[0].Step)
	}
	inline.Steps[1].Step = "changed-source"
	if inlineSteps[1].Step != "second" {
		t.Fatalf("AllSteps copy changed through inline result: %q", inlineSteps[1].Step)
	}

	overflow := action.SagaResult[int]{
		StepsCount: action.MaxSagaSteps + 1,
		StepsOverflow: []action.StepResult{
			{Step: "overflow"},
		},
	}
	overflow.Steps[0].Step = "inline"
	overflowSteps := overflow.AllSteps()
	overflowSteps[0].Step = "changed-inline-copy"
	overflowSteps[action.MaxSagaSteps].Step = "changed-overflow-copy"
	if overflow.Steps[0].Step != "inline" || overflow.StepsOverflow[0].Step != "overflow" {
		t.Fatalf("result changed through AllSteps copy: inline=%q overflow=%q", overflow.Steps[0].Step, overflow.StepsOverflow[0].Step)
	}
}

func TestBuilderDescribeReturnsIndependentMetadata(t *testing.T) {
	t.Parallel()

	builder := action.New("builder.describe", func(context.Context, int) (int, error) {
		return 0, nil
	}).Description("original").
		Tag("original-tag").
		RequireRole("original-role").
		RequirePermission("original-permission").
		RequireFeature("original-feature")

	meta := builder.Describe()
	meta.Description = "changed"
	meta.Tags[0] = "changed-tag"
	meta.RequiredRoles[0] = "changed-role"
	meta.RequiredPermissions[0] = "changed-permission"
	meta.RequiredFeatures[0] = "changed-feature"

	got := builder.Describe()
	if got.Description != "original" || got.Tags[0] != "original-tag" || got.RequiredRoles[0] != "original-role" || got.RequiredPermissions[0] != "original-permission" || got.RequiredFeatures[0] != "original-feature" {
		t.Fatalf("Describe exposed builder metadata: %+v", got)
	}

	var nilBuilder *action.Builder[int, int]
	if nilBuilder.Describe() != nil {
		t.Fatal("Describe on a nil Builder must return nil")
	}
}
