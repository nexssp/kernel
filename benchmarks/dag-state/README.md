# DAG state publication benchmark

This nested module compares three ways to publish DAG node outputs while retaining prior read views: an in-place mutable Go map, a copy-on-write map, and the kernel's internal persistent AVL map (`internal/pmap`). The AVL map creates immutable roots on update and shares unchanged tree structure across snapshots. Its source is imported through the kernel module path and resolved locally by the `replace` directive in this module's `go.mod`.

## Reproduce

Use Go 1.27.3 on Linux/amd64. From this directory, run:

```sh
go test ./...
go test -run '^$' -bench '^BenchmarkStatePublication$' -benchmem -benchtime=200ms -count=3
```

The benchmark matrix crosses 0, 128, or 1,024 initial keys with 16, 64, or 256 outputs. Each iteration retains one view per output. The mutable-map case intentionally mutates the same map and is a speed baseline only; `TestPublicationSnapshotBehavior` verifies that this view observes later writes. The COW case clones the whole map before each output. The AVL case stores the new persistent root after each output and retains earlier roots as snapshots. Initial structures and output keys are prepared outside the timer. The COW clone helper uses `make(map[string]any, 16)` followed by `maps.Copy`.

## Representative measurements

The following medians were measured on Linux/amd64 with Go 1.27.3 and an Intel Xeon processor at 2.50 GHz, using five 1-second samples for each displayed case. Reproduce these rows with:

```sh
go test -run '^$' -bench 'BenchmarkStatePublication/(map|cow|avl)/initial=(0|1024)/outputs=256' -benchmem -benchtime=1s -count=5
```

They are synthetic publication-only results, not production latency estimates. Shared-host load can affect timings; the linked decision record preserves the earlier 200 ms / three-sample measurements.

| Initial keys / outputs | Mutable map (incorrect snapshots) | Copy-on-write | Internal AVL |
|---|---:|---:|---:|
| 0 / 256 | 37.1 µs, 36.4 KB, 12 allocs | 5.12 ms, 3.79 MB, 2,236 allocs | 178.7 µs, 164.8 KB, 2,799 allocs |
| 1,024 / 256 | 178.3 µs, 159.4 KB, 19 allocs | 45.9 ms, 40.97 MB, 4,883 allocs | 256.7 µs, 231.4 KB, 3,840 allocs |

The mutable-map figures are only a speed baseline because that implementation mutates prior views. The persistent AVL retains snapshots and is faster and uses less memory than whole-map copy-on-write in these samples. For the original decision-time measurements and full interpretation, see [`docs/decisions/2026-11-08-internal-persistent-map.md`](../../docs/decisions/2026-11-08-internal-persistent-map.md).

## End-to-end dynamic DAG benchmark

The repository's dynamic DAG benchmark can be run from the kernel module root with:

```sh
go test -run '^$' -bench '^BenchmarkDynamicExecute$' -benchmem -benchtime=200ms -count=3 ./ai/dag
```

This benchmark exercises the executor in addition to the publication-only comparison above. Record fresh measurements for the current implementation and environment; the representative publication-only measurements above are not end-to-end execution results, and results from earlier HAMT-based implementations are not measurements of the current AVL map.
