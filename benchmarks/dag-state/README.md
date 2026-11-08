# DAG state publication benchmark

This nested module compares three ways to publish DAG node outputs while retaining prior read views: an in-place mutable Go map, a copy-on-write map, and the kernel's internal persistent AVL map (`internal/pmap`). The AVL map creates immutable roots on update and shares unchanged tree structure across snapshots. Its source is imported through the kernel module path and resolved locally by the `replace` directive in this module's `go.mod`.

## Reproduce

Use Go 1.27.3 on Linux/amd64. From this directory, run:

```sh
go test ./...
go test -run '^$' -bench '^BenchmarkStatePublication$' -benchmem -benchtime=200ms -count=3
```

The benchmark matrix crosses 0, 128, or 1,024 initial keys with 16, 64, or 256 outputs. Each iteration retains one view per output. The mutable-map case intentionally mutates the same map and is a speed baseline only; `TestPublicationSnapshotBehavior` verifies that this view observes later writes. The COW case clones the whole map before each output. The AVL case stores the new persistent root after each output and retains earlier roots as snapshots. Initial structures and output keys are prepared outside the timer. The COW clone helper uses `make(map[string]any, 16)` followed by `maps.Copy`.

## End-to-end dynamic DAG benchmark

The repository's dynamic DAG benchmark can be run from the kernel module root with:

```sh
go test -run '^$' -bench '^BenchmarkDynamicExecute$' -benchmem -benchtime=200ms -count=3 ./ai/dag
```

This benchmark exercises the executor in addition to the publication-only comparison above. Record fresh measurements for the current implementation and environment; results from earlier HAMT-based implementations are not measurements of the current AVL map.
