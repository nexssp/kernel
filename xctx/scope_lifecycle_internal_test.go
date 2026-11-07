package xctx

import (
	"context"
	"testing"
)

func TestLockScopeAtGenerationDetachesStaleMutation(t *testing.T) {
	scope := &RequestScope{}
	staleContext := WithScope(context.Background(), scope)
	staleGeneration, _ := staleContext.Value(scopeGenKeyT{}).(uint64)
	scope.Reset()
	liveContext := WithScope(context.Background(), scope)

	updatedContext, detachedScope := lockScopeAtGeneration(staleContext, scope, staleGeneration)
	if detachedScope == scope {
		t.Fatal("stale mutation retained the scope now bound to another context")
	}
	detachedScope.UserID = "stale-user"
	detachedScope.mu.Unlock()

	if got := UserIDFrom(liveContext); got != "" {
		t.Fatalf("live scope was mutated by stale setter: UserID=%q", got)
	}
	if got := UserIDFrom(updatedContext); got != "stale-user" {
		t.Fatalf("stale-derived context lost its own setter value: UserID=%q", got)
	}
}
