# Decision: persistent snapshots for DAG state

**Status:** Chosen for this patch. **Baseline:** public `main` at `e788453b83a9880127d07000ae4b75929c2f6c46`.

## Problem and behavior

Dynamic execution passes a `ReadState` view to each node. On public `main`, that view shares a mutable Go map with the executor, so a node that retains its input can observe outputs published by later steps. The regression now checks `dag.OutputKey("step")` (the actual output key), and it fails on the mutable-map baseline with `retained ReadState observed later output "done"`.

`State` now holds a persistent HAMT root. `Set`, `Clone`, and `CopyFrom` publish or share roots without changing an older `ReadState`; `CopyFrom` initially shares the source root, while later writes replace only the writer's root. Values remain shallow-shared, not deep-copied, so callers must not mutate values stored in snapshots. Nested-DAG persistence is not changed in this patch and remains a separate design decision.

**Known nested-DAG limitation:** on nested failure, `runNestedDAG` stores the partial inner `*State` as an `any` value under `InnerStateKey` in the parent HAMT. `State` itself remains mutable: a caller that retrieves that pointer and calls `Set` changes the root on the same object, so a later parent `Get` returns the mutated pointer. Persistent roots protect `ReadState` snapshots but do not deep-freeze pointer-valued entries. Resolving this requires making `State` immutable or introducing a distinct value-type snapshot; that design change is out of scope.

## Why a persistent map instead of map copy-on-write

The mutable-map baseline is fastest but violates the required snapshot behavior. Copy-on-write maps fix that bug, but each output copies all current keys. The reproducible standalone benchmark in [`benchmarks/dag-state`](../../benchmarks/dag-state/README.md) and the end-to-end `BenchmarkDynamicExecute` both show the cost. End-to-end medians from three runs include:

| Initial keys / outputs | Mutable map baseline | COW map | HAMT v0.7.0 |
|---|---:|---:|---:|
| 0 / 16 | 5.215 µs | 16.849 µs | 11.550 µs |
| 0 / 256 | 97.837 µs | 3.690 ms | 229.497 µs |
| 128 / 64 | 32.611 µs | 1.234 ms | 49.342 µs |
| 1,024 / 16 | 156.515 µs | 2.230 ms | 11.852 µs |
| 1,024 / 256 | 220.860 µs | 36.193 ms | 224.311 µs |

The measured impact is conditional on the workload, not a universal slowdown. In the no-op end-to-end runs, at 0 initial keys/16 outputs the mutable baseline measured 5.215 µs and HAMT 11.550 µs; at 0/256 they measured 97.837 µs and 229.497 µs; at 1,024/256 they measured 220.860 µs and 224.311 µs. In the standalone 256-output matrix, HAMT measured 120.894, 123.414, and 165.738 µs at 0, 128, and 1,024 initial keys; the mutable baseline measured 26.360, 32.852, and 141.871 µs. This shows the timings become closer as initial state grows, although the mutable baseline remains incorrect. These benchmarks use no-op actions: longer real action work can make state-publication cost a smaller share of total latency, while very short or routing-dominated workflows may expose it. Real action latency was not measured, so no percentage threshold or general slowdown is claimed.

The HAMT is not allocation-free: persistent updates create new version nodes. In the 1,024-key/256-output state-publication benchmark it uses 259,378 B and 1,854 allocations/op, versus 40,974,024 B and 4,883 allocations/op for COW. This still misses the zero-allocation ideal; with freely retainable `ReadState` snapshots, avoiding versioned storage would require a different ownership or arena design, not map-copy COW.

## Dependency trade-off and maintenance risk

The production dependency is pinned to [`github.com/arnonrgo/immutable` v0.7.0](https://github.com/Arnonrgo/immutable/releases/tag/v0.7.0). Its tagged module declares Go 1.26 and no module dependencies, so it removes the old `golang.org/x/exp` dependency required by Ben Johnson's upstream v0.4.3. The fork's v0.7.0 tag and current `master` HEAD were both `576067d6711b1053527c02b3ed08040b88eb896a`, dated 2026-02-24, at the 2026-11-07 check; there had been no later commit. It is MIT licensed and explicitly identifies itself as a fork of the upstream library.

Using this newer fork avoids the upstream package's old `x/exp` dependency, but it is not evidence of ongoing maintenance. The fork's ownership, compatibility with upstream, and quiet period since February 2026 are real maintenance risks. The dependency is pinned and isolated behind `ai/dag/state.go`, with regression tests and a reproducible benchmark to make replacement or rollback tractable.

Vendoring the MIT-licensed implementation into `internal/hamt/` was considered. That would keep the selected implementation source in the repository and remove the fork from the production module's dependency graph, but transfer updates, security review, and compatibility decisions to Kernel. This patch keeps the dependency pinned behind `ai/dag/state.go`; vendoring remains an alternative if availability or maintenance risk changes. No effort estimate is made.

The repository's depguard rules are a targeted package denylist, not a general maintenance-status policy. This patch explicitly denies the superseded `github.com/benbjohnson/immutable` upstream to prevent accidental reintroduction; the selected fork remains a documented, accepted risk. That specific rule does not imply that all low-activity dependencies are banned.

## Scope

This patch fixes the three approved follow-ups: the DSL cache-key collision, validation-before-action for `StateMachineAny`, and stable retained state views during DAG execution. It adds focused regression tests, this snapshot-semantics changelog entry, and the benchmark/decision artifacts. Nested-DAG pointer immutability, commits, pushes, and tags are out of scope.
