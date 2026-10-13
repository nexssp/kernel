package xfile_test

import (
	"context"
	"io"
	"testing"

	"github.com/nexssp/kernel/xfile"
)

type resolverFunc func(context.Context, xfile.Ref) (io.ReadCloser, xfile.Info, error)

func (f resolverFunc) Open(ctx context.Context, ref xfile.Ref) (io.ReadCloser, xfile.Info, error) {
	return f(ctx, ref)
}

var _ xfile.Resolver = resolverFunc(nil)

func TestRefUsesOpaqueTypedFields(t *testing.T) {
	ref := xfile.Ref{
		Provider:       xfile.Provider("object-store"),
		ID:             xfile.ID("opaque-id"),
		Version:        xfile.Version("v1"),
		ExpectedSize:   xfile.ExpectedSize(42),
		ExpectedDigest: xfile.ExpectedDigest("sha256:example"),
	}
	if ref.Provider == "" || ref.ID == "" || ref.Version == "" || ref.ExpectedSize <= 0 || ref.ExpectedDigest == "" {
		t.Fatalf("Ref fields not retained: %#v", ref)
	}

	info := xfile.Info{
		Provider: ref.Provider,
		ID:       ref.ID,
		Version:  xfile.Version("v1"),
		Size:     42,
		Digest:   "sha256:example",
	}
	if info.Provider != ref.Provider || info.ID != ref.ID || info.Size != int64(ref.ExpectedSize) || info.Digest != string(ref.ExpectedDigest) || info.Version != ref.Version {
		t.Fatalf("confirmed Info fields not retained: %#v", info)
	}
}
