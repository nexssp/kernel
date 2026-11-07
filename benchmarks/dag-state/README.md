# DAG state publication benchmark

This module compares three ways to publish DAG node outputs while nodes retain the `ReadState` view they received: the mutable Go map used at public `main`, a copy-on-write map, and a persistent HAMT using `github.com/arnonrgo/immutable` v0.7.0.

Keep this module's HAMT pin synchronized with the root `go.mod`; when the production pin changes, update this module and regenerate the benchmark tables.

## Reproduce

Use Go 1.27.3 on Linux/amd64. From this directory, run:

```sh
go test ./...
go test -run '^$' -bench '^BenchmarkStatePublication$' -benchmem -benchtime=200ms -count=3
```

The full matrix is 0, 128, or 1,024 initial keys crossed with 16, 64, or 256 outputs. The benchmark retains one read view per output. The `map` case intentionally mutates the same map and is a speed baseline only; `TestPublicationSnapshotBehavior` confirms that this view sees later writes. The COW case clones the entire map before every output. The HAMT case stores a new persistent root on each `Set`. The initial structures are prepared outside the timer; the map clone helper matches public-main `State.Clone` (`make(map, 16)` followed by `maps.Copy`).

The benchmark was run with Go 1.27.3 on Linux/amd64, Intel Xeon @ 2.50 GHz, against public-main SHA `e788453b83a9880127d07000ae4b75929c2f6c46`. Values below are medians of three runs; time is ns/op converted for readability.

| Initial keys | Outputs | Mutable map | COW map | HAMT v0.7.0 |
|---:|---:|---:|---:|---:|
| 0 | 16 | 0.719 µs | 12.287 µs | 5.586 µs |
| 0 | 64 | 6.329 µs | 184.139 µs | 22.678 µs |
| 0 | 256 | 26.360 µs | 3.870 ms | 120.894 µs |
| 128 | 16 | 15.264 µs | 269.628 µs | 6.358 µs |
| 128 | 64 | 16.442 µs | 1.165 ms | 25.469 µs |
| 128 | 256 | 32.852 µs | 8.021 ms | 123.414 µs |
| 1,024 | 16 | 134.509 µs | 2.836 ms | 8.839 µs |
| 1,024 | 64 | 134.148 µs | 10.472 ms | 35.119 µs |
| 1,024 | 256 | 141.871 µs | 44.312 ms | 165.738 µs |

For example, at 1,024 initial keys and 256 outputs, the timed state-publication paths allocated 159,432 B / 19 allocations for the mutable-map baseline, 40,974,024 B / 4,883 allocations for COW, and 259,378 B / 1,854 allocations for the HAMT. The baseline is not snapshot-safe; it is included only to expose the cost of the correct approaches relative to in-place mutation.

## End-to-end dynamic DAG check

The patch also includes `ai/dag/dynamic_state_bench_test.go`, which benchmarks the real dynamic DAG executor using the same 0/128/1,024 initial-key and 16/64/256-output matrix. Run the candidate implementation from the repository root with:

```sh
go test -run '^$' -bench '^BenchmarkDynamicExecute$' -benchmem -benchtime=200ms -count=3 ./ai/dag
```

For the comparison below, the same runner was copied into clean worktrees at the public-main SHA; the COW variant cloned `currentState` before each output `Set`, and the HAMT variant is the patch. The mutable-map baseline fails the corrected retained-view regression, so its timing is not a correctness candidate.

| Initial keys | Outputs | Public-main map | COW map | HAMT v0.7.0 |
|---:|---:|---:|---:|---:|
| 0 | 16 | 5.215 µs | 16.849 µs | 11.550 µs |
| 0 | 64 | 25.299 µs | 207.108 µs | 45.330 µs |
| 0 | 256 | 97.837 µs | 3.690 ms | 229.497 µs |
| 128 | 16 | 22.035 µs | 276.949 µs | 11.710 µs |
| 128 | 64 | 32.611 µs | 1.234 ms | 49.342 µs |
| 128 | 256 | 108.425 µs | 7.457 ms | 230.710 µs |
| 1,024 | 16 | 156.515 µs | 2.230 ms | 11.852 µs |
| 1,024 | 64 | 178.525 µs | 8.890 ms | 45.574 µs |
| 1,024 | 256 | 220.860 µs | 36.193 ms | 224.311 µs |

The state-publication module is the directly reproducible side-by-side algorithm comparison. The end-to-end benchmark confirms that the same scaling appears in the executor, where action and routing overhead also contribute.
