// Package xfile defines the provider-neutral contract for opening externally
// stored files. References are opaque identifiers, never operating-system paths.
package xfile

import (
	"context"
	"io"
)

// Provider identifies the storage provider that owns a file. Its value is a
// routing hint, not an authorization credential.
type Provider string

// ID is an opaque provider-scoped file identifier. It must not contain or be
// interpreted as an operating-system path.
type ID string

// Version identifies a provider's immutable file version when available.
type Version string

// ExpectedSize is an optional size supplied by a caller as an integrity hint.
// Resolvers must verify it against the opened object when it is non-zero.
type ExpectedSize int64

// ExpectedDigest is an optional algorithm-prefixed digest supplied by a caller
// as an integrity hint (for example, "sha256:<hex>"). Resolvers must verify it
// against the opened object when it is non-empty.
type ExpectedDigest string

// Ref identifies a file without exposing a storage path. All fields are
// untrusted input: in particular, none of them grants access to the file.
type Ref struct {
	Provider       Provider
	ID             ID
	Version        Version
	ExpectedSize   ExpectedSize
	ExpectedDigest ExpectedDigest
}

// Info contains metadata confirmed for the object actually opened. Provider,
// ID, Version, Size, and Digest identify the resolved object; Size and Digest
// are authoritative results, distinct from Ref's caller-supplied expectations.
// Digest uses an explicit algorithm prefix (for example, "sha256:<hex>").
type Info struct {
	Provider Provider
	ID       ID
	Version  Version
	Size     int64
	Digest   string
}

// Resolver opens a referenced file. Implementations must derive caller and
// tenant authorization exclusively from ctx (using ScopeFromContext); Ref is
// untrusted selection/integrity data and must never be used as authorization.
// Implementations should classify errors with kernel/xerr. On success, the
// caller owns and must close the returned reader.
type Resolver interface {
	Open(context.Context, Ref) (io.ReadCloser, Info, error)
}
