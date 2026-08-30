package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestBuilder_MetadataAndRoute(t *testing.T) {
	t.Parallel()
	b := action.New("unknown", func(ctx context.Context, req int) (int, error) {
		return req, nil
	}).
		Name("user.create").
		Description("Creates a new user account").
		Tag("user", "write", "critical").
		Route("http.POST /users")

	act := b.Build()
	meta := act.GetMeta()

	if meta.Name != "user.create" {
		t.Errorf("expected user.create, got %q", meta.Name)
	}
	if meta.Description == "" {
		t.Error("expected description to be set")
	}
	if len(meta.Tags) != 3 || meta.Tags[2] != "critical" {
		t.Errorf("expected 3 tags, got %v", meta.Tags)
	}
	if len(act.GetBindings()) != 1 {
		t.Errorf("expected 1 binding, got %v", act.GetBindings())
	}
}
