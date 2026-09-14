// Package xfs provides filesystem primitives for Nexss workspaces.
//
// Three layers, two threat models:
//
//   - WriteFile / WriteFileAtomic(path, ...) — trusted path, no
//     validation. Use only for paths your code builds from constants
//     (config dirs, cache roots, temp dirs). WriteFile is a thin
//     wrapper over os.WriteFile; WriteFileAtomic is temp+fsync+rename.
//
//   - Rel(p) — string-level validation of an untrusted path. Fast,
//     typed errors. Rejects absolute paths, "..", ":", NUL, control
//     chars, trailing dots, Windows reserved device names.
//
//   - Root — kernel-enforced containment. Opens a directory handle via
//     os.OpenRoot; every path goes through Rel first, then through the
//     kernel, which refuses symlink escapes and TOCTOU races that Rel
//     alone cannot see. Any path derived from user input, HTTP, config,
//     or plugin manifests MUST go through Root.
package xfs
