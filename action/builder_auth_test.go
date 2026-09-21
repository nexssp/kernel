package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestRequireRole(t *testing.T) {
	t.Parallel()
	act := ktest.NewOkAction("admin.only").RequireRole("admin").Build()

	// Rejects unauthenticated/missing role
	ktest.Run(t, act, context.Background(), struct{}{}).
		ErrorKind(xerr.KindForbidden)

	// Accepts valid role
	ktest.Run(t, act, ktest.CtxWithRole(t, "admin"), struct{}{}).
		NoError().
		Equals("ok")
}

func TestRequireFeature(t *testing.T) {
	t.Parallel()
	act := ktest.NewOkAction("beta.feature").RequireFeature("beta-ui").Build()

	ctx, scope := ktest.Ctx(t)
	scope.Features = []string{"beta-ui"}

	ktest.Run(t, act, ctx, struct{}{}).
		NoError().
		Equals("ok")
}
