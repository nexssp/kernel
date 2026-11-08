# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Clamp `SagaResult.AllSteps` to a non-negative `StepsCount` and the available overflow entries, avoiding panics for inconsistent exported values.

## [1.0.0-rc1]

This entry covers changes since `v0.37.7` (`726f7a8`), through `v0.37.9`. It does not repeat behavior already present in that base release.

### Added

- Add memory-bounded generic `ringbuf.Buffer` constructors: `NewBufferChecked[T]` and `NewBufferWithOptions[T]`, with the `BufferOptions` type. Add checked and explicit-options constructors for action history and the in-memory observation sink, plus `WithHistoryChecked`, `WithHistoryWithOptions`, `RecordHistoryChecked`, and `RecordHistoryWithOptions` builder methods.
- Add `stream.ErrInvalidTickRate` and `stream.TicksChecked` to validate tick rates before iteration.
- Add `xctx.WithIdempotencyKey` and `xctx.IdempotencyKeyFrom` for explicit request de-duplication keys, plus `IdempotencyConfig.RequestKeyFunc` for typed requests.
- Add explicit-limit variants: `AsyncOptions` with `FanOutWithOptions` and `RaceWithOptions`; `ParallelWithOptions`, `ParallelNamedWithOptions` and `ParallelMapWithOptions`; `DurableOptions` with `DurableWithOptions` and `Builder.DurableWithOptions`; `SharedFlightOptions` with `CoalesceMiddlewareWithOptions`; and `IdempotencyConfig.FlightTimeout`.
- Add `Builder.WithDrainSupervisor`, which rejects new calls before hooks and middleware once draining begins and includes the whole action lifecycle in the drain wait.
- Add package documentation for `node` and `observe`.
- CI: `govulncheck`, a portability job (cross-platform `go vet` and a 32-bit test run), and a tag-triggered release workflow that publishes hyphenated tags such as `-rc1` as pre-releases.

### Changed

- **Idempotency compatibility change:** idempotency now runs only when a caller supplies a key. Transport adapters should read `cfg.Header()` (default `Idempotency-Key`) and bind it with `xctx.WithIdempotencyKey`; `RequestID` is correlation-only, and payload hashes are not fallback keys. `KeyFunc` remains available for `[]byte` requests.
- Builder-created idempotent actions use the final action name as their default namespace. Versioned v2 storage keys include the action namespace and a fingerprint of tenant, user, and authorization scope. Existing persisted entries use a different key format and will not replay; expire or migrate them during deployment. Direct middleware use requires an explicit namespace.
- Same-process idempotency flights are bounded by `MaxConcurrentFlights` (default 256). Shared work executes synchronously using the initiating caller's context values but not its cancellation; `FlightTimeout` defaults to 2 minutes. Each caller can cancel its own wait, while the shared work can finish for healthy followers. This avoids background handler goroutines.
- DAG state now uses the in-repository persistent AVL map in `internal/pmap` instead of `github.com/arnonrgo/immutable`; snapshots and clones share immutable roots and diverge on writes. The external dependency is removed from the module graph.
- Generic mutex-backed buffer allocations now use a default 64 MiB per-allocation budget. `ringbuf.NewBuffer[T]`, `action.NewHistory`, and `observe.NewMemorySink` return nil when allocation validation fails. The `WithHistory` convenience method returns a nil history handle on rejection, and `Build().Do` returns a configuration error before running the action when requested history could not be configured. Use checked constructors to handle errors or options constructors to choose a budget. For zero-size buffer elements, no per-element bytes are counted, so the byte budget does not cap logical capacity. These convenience forms remain available and are not marked `Deprecated:`.
- `action.Durable` and `action.Idempotent` normalize successful empty-interface results through JSON before returning them, including named empty-interface result types. This makes the first result match replay values (for example, numbers become `float64`, objects become `map[string]any`, arrays become `[]any`, and typed nil pointers, maps, and slices become nil). Concrete result types follow their declared JSON decoding behavior. A non-empty interface generally cannot be reconstructed from JSON as its original concrete dynamic type; use a concrete result type when replay must succeed.
- Exponential backoff now doubles with overflow-safe integer arithmetic and saturates at the configured maximum instead of using a floating-point calculation and a fixed attempt cutoff. `ExponentialBackoff` and `ExponentialJitter` normalize negative base and maximum durations to zero and cap the base at the maximum.
- `stream.Ticks` yields `ErrInvalidTickRate` once for positive rates above 1 GHz instead of panicking. Nonpositive rates retain the legacy empty-sequence behavior; `TicksChecked` returns an error for every invalid rate.
- **Default limits (behavior change):** several concurrency helpers now bound work by default. `FanOut` and `Race` use at most 16 concurrent items (`DefaultAsyncMaxConcurrency`) and a 2-minute whole-call timeout (`DefaultAsyncTimeout`); a non-positive `maxConcurrency` previously meant unbounded. `Durable` steps allow 16 concurrent unique executions per middleware (`DefaultDurableMaxConcurrentExecutions`) with a 2-minute flight timeout (`DefaultDurableTimeout`) that includes the wait for a slot. `Coalesce` and `Dedup` leaders are bounded by `DefaultSharedFlightTimeout` (2 minutes). Use the `*WithOptions` variants for long-running or high-fan-out workloads.
- `Coalesce` keys are namespaced by action name. `Coalesce`, `Dedup`, and `Idempotency` run shared work with the initiating caller's values but detached from its cancellation, bounded by their shared-flight timeout. Caller cancellation no longer cancels work that healthy followers may need; because the leader remains synchronous, its call may not return until the work completes or the shared timeout expires. This changes the previous cancellation behavior and should be reviewed as a release contract.
- `Durable` and `DurableWithResume`: a journal payload that is invalid JSON or no longer decodes into the current result type now returns an `[Internal]` error instead of silently re-running the step.
- `Saga` checks the context before every step, runs each compensation under its own timeout (`DefaultSagaCompensationTimeout`, 2 minutes), and reports `RolledBack` only when compensation ran, no earlier successful step lacked an `Undo`, and no compensation failed.
- `StateMachine` copies its transition table at build time and restores the original state when the handler returns an error.
- `Hedge` no longer launches a hedge attempt when the primary has just completed.
- `Assign`/`Coerce` return an error for cyclic input values instead of recursing without bound.
- Dynamic DAG routing re-executes a node when a router revisits it within the same run, including when its output was present in the initial state; a persisted output is skipped only on that node's first visit in a resumed run.

### Documentation

- Clarify that journal-record and idempotency-completion errors may be ambiguous: a write may have committed before returning an error, so durable execution alone does not guarantee exactly-once external side effects.
- Clarify that dynamic DAG routers are re-evaluated on resumed execution and should select the same route for the same persisted state and stable graph configuration when a workflow must resume on the same branch.
- State that the library does not expose a runtime version API; applications can inspect Go build information when they need linked module-version metadata.
- Document the external-map dependency provenance and its replacement with the internal persistent AVL implementation.
- Replace the "pre-1.0" stability text in `README.md` and `SECURITY.md` with the release-candidate and SemVer policy, and correct the `FanOut` concurrency description.

### Fixed

- Idempotency storage keys no longer collide across default action namespaces, tenants, users, or authorization scopes. Invalid keys and corrupted/missing saved request hashes fail closed.
- Standalone and adaptive circuit breakers count caller deadline expirations as failures while continuing to exclude explicit cancellation. Breaker permits also complete when a handler exits with `runtime.Goexit`, so a half-open probe is not left wedged by that exit.
- Adaptive circuit-breaker timeouts now use overflow-safe integer arithmetic and saturate at the maximum `time.Duration`.
- `action.Testable.ExpectErr` now reports a clear diagnostic when it encounters a typed-nil `*xerr.AppError`.
- `action` tests and benchmarks now compile and pass on 32-bit platforms (`GOARCH=386`, `arm`): a benchmark passed an `int` burst of `1e15`, and the adaptive-timeout saturation test assumed a 64-bit `int`.
- `action` package documentation no longer begins with the license header on pkg.go.dev.
- Idempotency scope fingerprints now treat roles, permissions, and features as unordered sets, so reordering these lists does not re-execute a retry.
- `ParallelMap` now returns an error for duplicate action names instead of silently dropping an action, and `BranchAny` tag order is deterministic.
- `SagaResult` JSON includes only populated inline `steps` entries; the public fixed-size Go array and `steps_overflow` field remain unchanged.
- `Assign`/`Coerce` reject fractional floating-point values for integer destinations instead of truncating them.
- Dynamic DAG resume can revisit and rerun an already-persisted node after the first visit, fixing resumed revise loops.
- `Durable` rejects an empty step name with a validation error instead of silently running without journaling.

### Known issues

- `Saga.Undo` receives only the original request, not the step result.
- Several exported helpers have no test coverage (for example `Exclusive`, `LeaderOnly`, `ParallelNamed`, `ParallelMap`, `BranchAny`, `HashRouterAny`).

### Notes for upgraders

- `xerr.ServiceUnavailable` is deprecated in favour of `xerr.Unavailable`.
- `observe.NewJSONLSink` and `dag.Builder.AddNode` accept but ignore one parameter (a legacy size argument and a display name, respectively).

[Unreleased]: https://github.com/nexssp/kernel/compare/v1.0.0-rc1...HEAD
[1.0.0-rc1]: https://github.com/nexssp/kernel/compare/v0.37.7...v1.0.0-rc1
