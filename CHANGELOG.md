# Changelog

## [1.0.0-rc1] - 2026-11-08

This entry covers changes since `v0.37.7` (`726f7a8`). It does not repeat behavior already present in that base release.

### Added

- Add memory-bounded generic `ringbuf.Buffer` constructors: `NewBufferChecked[T]` and `NewBufferWithOptions[T]`, with the `BufferOptions` type. Add checked and explicit-options constructors for action history and the in-memory observation sink, plus `WithHistoryChecked`, `WithHistoryWithOptions`, `RecordHistoryChecked`, and `RecordHistoryWithOptions` builder methods.
- Add `stream.ErrInvalidTickRate` and `stream.TicksChecked` to validate tick rates before iteration.
- Add `xctx.WithIdempotencyKey` and `xctx.IdempotencyKeyFrom` for explicit request de-duplication keys, plus `IdempotencyConfig.RequestKeyFunc` for typed requests.

### Changed

- **Idempotency compatibility change:** idempotency now runs only when a caller supplies a key. Transport adapters should read `cfg.Header()` (default `Idempotency-Key`) and bind it with `xctx.WithIdempotencyKey`; `RequestID` is correlation-only, and payload hashes are not fallback keys. `KeyFunc` remains available for `[]byte` requests.
- Builder-created idempotent actions use the final action name as their default namespace. Storage keys are versioned and include the action namespace and a fingerprint of tenant, user, and authorization scope. Existing persisted entries use a different key format and will not replay; expire or migrate them during deployment. Direct middleware use requires an explicit namespace.
- Same-process idempotency flights are bounded by `MaxConcurrentFlights` (default 256). The first caller executes synchronously and owns the handler context; its cancellation is observed by the handler and remaining waiters receive the shared result. A follower can cancel its own wait without canceling the leader. This avoids background or detached handler goroutines.
- DAG state now uses the in-repository persistent AVL map in `internal/pmap` instead of `github.com/arnonrgo/immutable`; snapshots and clones share immutable roots and diverge on writes. The external dependency is removed from the module graph.
- Generic mutex-backed buffer allocations now use a default 64 MiB per-allocation budget. `ringbuf.NewBuffer[T]`, `action.NewHistory`, and `observe.NewMemorySink` return nil when allocation validation fails. The `WithHistory` convenience method returns a nil history handle on rejection, and `Build().Do` returns a configuration error before running the action when requested history could not be configured. Use checked constructors to handle errors or options constructors to choose a budget. For zero-size buffer elements, no per-element bytes are counted, so the byte budget does not cap logical capacity. These convenience forms remain available and are not marked `Deprecated:`.
- `action.Durable` and `action.Idempotent` normalize successful empty-interface results through JSON before returning them, including named empty-interface result types. This makes the first result match replay values (for example, numbers become `float64`, objects become `map[string]any`, arrays become `[]any`, and typed nil pointers, maps, and slices become nil). Concrete result types follow their declared JSON decoding behavior. A non-empty interface generally cannot be reconstructed from JSON as its original concrete dynamic type; use a concrete result type when replay must succeed.
- Exponential backoff now doubles with overflow-safe integer arithmetic and saturates at the configured maximum instead of using a floating-point calculation and a fixed attempt cutoff. `ExponentialBackoff` and `ExponentialJitter` normalize negative base and maximum durations to zero and cap the base at the maximum.
- `stream.Ticks` yields `ErrInvalidTickRate` once for positive rates above 1 GHz instead of panicking. Nonpositive rates retain the legacy empty-sequence behavior; `TicksChecked` returns an error for every invalid rate.

### Documentation

- Clarify that journal-record and idempotency-completion errors may be ambiguous: a write may have committed before returning an error, so durable execution alone does not guarantee exactly-once external side effects.
- Clarify that dynamic DAG routers are re-evaluated on resumed execution and should select the same route for the same persisted state and stable graph configuration when a workflow must resume on the same branch.
- State that the library does not expose a runtime version API; applications can inspect Go build information when they need linked module-version metadata.
- Document the external-map dependency provenance and its replacement with the internal persistent AVL implementation.

### Fixed

- Idempotency storage keys no longer collide across default action namespaces, tenants, users, or authorization scopes. Invalid keys and corrupted/missing saved request hashes fail closed.
- Standalone and adaptive circuit breakers count caller deadline expirations as failures while continuing to exclude explicit cancellation. Breaker permits also complete when a handler exits with `runtime.Goexit`, so a half-open probe is not left wedged by that exit.
- Adaptive circuit-breaker timeouts now use overflow-safe integer arithmetic and saturate at the maximum `time.Duration`.
- `action.Testable.ExpectErr` now reports a clear diagnostic when it encounters a typed-nil `*xerr.AppError`.
