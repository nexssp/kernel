# Nexss Kernel
[![Go Reference](https://pkg.go.dev/badge/github.com/nexssp/kernel.svg)](https://pkg.go.dev/github.com/nexssp/kernel)

Runnable examples are maintained in the central [Nexss Ecosystem Examples repository](https://github.com/nexssp/examples/tree/main/kernel).

Nexss Kernel is a typed Go library for building actions, middleware pipelines, and DAG workflows. Applications can provide transport, persistence, AI vendor, and identity-provider integrations.

The Kernel contains reusable execution primitives and middleware; application-specific adapters and domain logic belong in the application or sibling modules.

## Core Packages

| Package | Purpose |
| :--- | :--- |
| `action` | Typed action builders, middleware, composition, retries, timeouts, circuit breakers, concurrency limits, caching, idempotency, durable journaling/resume, streaming, sagas, and resilience policies. |
| `ai/dag` | Typed directed acyclic graphs with deterministic compilation, static and conditional routing, layered parallel execution, nested graphs, state snapshots, Mermaid rendering, and suspension/resume semantics. |
| `stream` | Domain-neutral lazy stream operators, including Map, Filter, Take, Batch, BatchByTime, Window, Reduce, FlatMap, Distinct, Throttle, Debounce, and worker pools. |
| `observe` | Optional vendor-neutral lifecycle events, bounded memory history, metrics aggregation, Prometheus, JSONL, and `slog` sinks. |
| `xctx` | Typed request context values, pooled request scopes, request/execution/trace identity, correlation data, roles, permissions, features, progress, and deterministic controls. |
| `xerr` | Categorized errors for validation, authorization, conflicts, not-found cases, transient failures, internal failures, and panic recovery. |
| `xfile` / `xfs` | Typed file references and resolvers, plus confined and atomic filesystem helpers. |
| `broadcast` / `ringbuf` | In-process topics and bounded ring buffers for event publication and telemetry. |
| `timing` | Timing-wheel scheduling. |
| `xtest/ktest` | Allocation checks, polling/leak helpers, test contexts, action fakes, contract assertions, and fluent action test runs. |

### Ring buffer memory budgets

`ringbuf.NewChecked(capacity)` validates the rounded allocation against the default 64 MiB per-ring budget before allocating and returns an error for invalid or over-budget capacities. For an explicit per-ring override, use `ringbuf.NewWithOptions(ringbuf.Options{Capacity: ..., MemoryBudgetBytes: ...})`. `ringbuf.New(capacity)` uses the default budget and returns `nil` when validation fails; over-budget requests are never clamped to a smaller ring.

The generic mutex-backed `ringbuf.NewBuffer[T](capacity)` uses the same default budget and returns `nil` on rejection. Use `NewBufferChecked[T]` to inspect an error or `NewBufferWithOptions[T]` to set an explicit per-buffer budget. The estimate includes the buffer header and backing array, but excludes allocator overhead and objects reachable through pointers in `T`. For zero-size `T`, no per-element bytes are counted, so the byte budget does not cap logical capacity. `action.NewHistory` and `observe.NewMemorySink` use the same budget; use `action.NewHistoryChecked` / `action.NewHistoryWithOptions` or `observe.NewMemorySinkChecked` / `observe.NewMemorySinkWithOptions` to inspect errors or select explicit budgets. Builder methods `WithHistoryChecked` / `RecordHistoryChecked` return allocation errors, while their `WithOptions` variants accept explicit limits. Legacy `RecordHistory` causes `Do` to return a configuration error rather than run without the requested history.

---

## Basic Action Usage

Actions are typed generic functions wrapped with fluent middleware pipelines. Request and response types are inferred from the handler signature:

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/nexssp/kernel/action"
)

type UserRequest struct {
    ID string `json:"id"`
}

type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

func main() {
    lookupUser := action.New("user.lookup", func(ctx context.Context, req UserRequest) (User, error) {
        return User{ID: req.ID, Name: "Alice"}, nil
    }).
        Timeout(2 * time.Second).
        Retry(2, action.ExponentialBackoff(25*time.Millisecond, 2*time.Second)).
        Build()

    user, err := lookupUser.Do(context.Background(), UserRequest{ID: "usr-42"})
    if err != nil {
        panic(err)
    }

    fmt.Printf("User: %s (%s)\n", user.Name, user.ID)
}
```

---

## Dynamic Dispatch and Type-Erased Calls

Use type-erased entry points when requests arrive as dynamic payloads, while retaining typed action handlers:

```go
act := action.New("user.lookup", lookupHandler).Build()

// 1. Pass a dynamically typed request to a typed action.
result, err := act.DoAny(ctx, UserRequest{ID: "usr-42"})

// 2. Universal Invoker: automatically decodes and coerces dynamic payloads
result, err = action.InvokeAny(ctx, act, map[string]any{"id": "usr-42"})

// 3. Coerce: converts dynamic structures (JSON, maps, strings) into target types
req, err := action.Coerce[UserRequest](untypedInput)

// 4. Dynamic lifting: lifts AnyAction into *Builder[any, any] for composition
dynamicAct := action.Dynamic(act).Build()
```

When converting `map[string]any` to a string, `Coerce`, `Assign`, and `DoAny` check the names in `DefaultTextFields` in order. It is a shared mutable package variable; do not mutate it concurrently with coercion calls.

---

## Composition and Pipelines

- `Pipe[Req, Mid, Res]` connects two actions sequentially (`A → B`).
- `PipeWith[Req, Mid1, Mid2, Res]` connects two actions with an inline transformation function, without an intermediate adapter struct.
- `Chain[T]` executes same-typed actions sequentially, piping output to input.
- `Branch` and `BranchAny` route execution conditionally using a router function.
- `FirstSuccess` runs fallback chains, returning the first non-error result.
- `Parallel` executes typed actions concurrently over the same request (scatter-gather).
- `ParallelNamed` and `ParallelMap` execute heterogeneous actions concurrently, collecting results into a `map[string]any`.

```go
// Pipeline with an inline field transform
pipeline := action.PipeWith("user.process",
    fetchUserAct,
    func(ctx context.Context, u User) (string, error) { return u.Email, nil },
    sendEmailAct,
).Build()

// Heterogeneous concurrent execution
reviewer := action.ParallelNamed[string]("review.suite", map[string]action.AnyAction{
    "security": checkSecurityAct,
    "linter":   runLinterAct,
}).Build()

results, err := reviewer.Do(ctx, "package main...")
```

---

## Resilience and Concurrency

### Timeouts
- A positive `Timeout(d)` adds timeout middleware. Zero adds no middleware and does not remove an earlier timeout: `.Timeout(time.Second).Timeout(0)` retains the first wrapper while setting `Meta.Timeout` to zero. A negative duration invalidates builder configuration. `ApplyResilience` rejects negative durations, and direct `TimeoutMiddleware` use returns a validation error when the action is invoked with a negative duration.

Timeout scope follows middleware order: fluent `.Timeout(d).Retry(...)` wraps the retry sequence, while `.Retry(...).Timeout(d)` applies the timeout to each attempt. `ApplyResilience` uses per-attempt ordering; middleware registered before it wraps its configured retry/timeout policies, a configured breaker wraps that chain, and configured concurrency admission is outermost. `WithDSL` wraps its retry sequence.

### Retries and Circuit Breaking
- `Retry` retries errors classified as transient by `xerr.IsTransient`; that classification alone does not make an operation safe to repeat.
- `RetryIf` evaluates an application-defined `RetryPredicate(err error) bool`.
- `RetryAll` retries on any non-nil error (intended for idempotent batch jobs).
- `InferredResilient` / `SmartResilience` combines an adaptive circuit breaker with exponential-jitter retry; its predicate skips errors classified as permanent by `xerr.IsPermanent`.
- `Adaptive` applies a circuit breaker with dynamic timeout scaling and half-open state recovery.
- Standalone and adaptive breakers count caller deadline expirations as failures; explicit caller cancellation is excluded. A handler that ignores its context can keep a half-open probe occupied until it returns.

Retries can execute a handler more than once. `Race` may start multiple candidates and cancels losers, but cancellation is cooperative and cannot undo side effects already performed; use retries and hedged calls for idempotent work or protect duplicate side effects.

### Backoff Strategies
- `ExponentialBackoff(base, max)` doubles wait duration per attempt.
- `ExponentialJitter(base, max)` adds ±30% random jitter to mitigate thundering-herd spikes.
- `LinearBackoff(step)` and `ConstantBackoff(duration)`.

### Concurrency and Admission Control
- `Dedup(keyFn)` applies singleflight deduplication. Callers sharing a key block until active execution completes, receiving copies of the result. Context cancellation of a joined caller does not cancel the flight.
- `Coalesce(coalescer, keyFn)` shares in-flight results across actions. A canceled waiter may exit without terminating the background execution for other callers.
- `ConcurrencyLimit(limit)` enforces an in-process ceiling using atomic counters. Excess calls return `ErrConcurrencyLimit`.
- `RateLimit`, `RateLimitWithKey`, and `RateLimitDistributed` provide token-bucket admission control.
- `AdaptiveLoadShedding` evaluates system statistics (CPU percentage and goroutine count) against priority tiers (`PriorityCritical`, `PriorityNormal`, `PriorityLow`), dropping non-essential traffic when thresholds are exceeded.

### Execution Primitives
- `FanOut(ctx, act, reqs, maxConcurrency)` executes concurrent items in input order. A nil or empty request slice returns a nil result slice. A non-positive concurrency limit uses `DefaultAsyncMaxConcurrency` (16), and the whole call is bounded by `DefaultAsyncTimeout` (2 minutes) unless the parent context ends sooner; use `FanOutWithOptions` to change either.
- `Race(ctx, act, reqs)` returns the first successful result and cancels losing attempts. Handlers must cooperate with context cancellation.

---

## Caching and Idempotency

### Multi-Layer Read-Through Caching
`Cache(ttl, keyFn, layers...)` coordinates L1 (local memory) through LN (distributed) stores:
- Cache hits return without invoking the underlying handler.
- Cache misses execute the handler under singleflight protection to prevent cache stampedes.
- A hit in a later cache layer can back-fill earlier layers.
- If no layers are passed, an internal thread-safe in-memory cache is instantiated. It evicts entries inline during `Set` and lazily during `Get`; it starts no janitor goroutine and requires no `Close()` call.
- `Once()` memoizes the first execution's result or error indefinitely for static configurations, including cancellation; it runs under the first caller's context.

### Middleware with idempotency
- `Use` and `UseWithDispatcher` wrap each action invocation outside the idempotency replay check. They run on successful replays too, so put authentication, authorization, rate limits, validation, and admission checks here. The built-in authentication/authorization guards and `RateLimit*` builder methods use this per-invocation path.
- Per-invocation middleware may reject a request; if it returns a successful cached or fallback response without calling `next`, idempotency cannot validate that request's key or payload. Put response-producing caches and similar short-circuits on the execution-only path.
- `UseOnExecution` and `UseWithDispatcherOnExecution` place middleware inside idempotency. A successful replay skips them; use these for response caching, transactions, durable memoization, locks, or other work that should happen only when the action actually executes. Do not place authentication or authorization checks on this path.
- With idempotency enabled, the per-invocation chain wraps the idempotency check, which wraps the execution-only chain. Their order is preserved within each chain. Without idempotency, both kinds run in builder registration order.

### Idempotency Contracts
- `.Idempotent()` and `.IdempotentWithConfig(cfg)` enable replay only when a caller supplies an explicit key. Transport adapters should read `cfg.Header()` (default `Idempotency-Key`) and bind it with `xctx.WithIdempotencyKey(ctx, key)`. `RequestID` is correlation-only; neither it nor a payload hash is used as a fallback key. For typed requests, configure `RequestKeyFunc`; the older `KeyFunc` remains available for `[]byte` requests.
- Builder-created actions default `Namespace` to their final action name; direct `IdempotencyMiddleware` use requires an explicit namespace.
- Idempotency storage keys are derived from the namespace, caller scope, and client key rather than storing the raw client key. Entries persisted under the previous key format are not found with this format; plan migration or expiration when upgrading from it.
- `MemoryIdempotencyStore` provides in-memory reference coordination.
- Without a configured store, each `.Idempotent()` configuration creates an in-memory store attached to its builder; separately configured actions get separate stores, while builds from the same configured builder reuse its store. It is not persisted across restarts, holds up to 65,536 entries, and can evict unexpired entries at capacity; supply an explicit shared store when this scope or capacity does not fit the application.
- `IdempotencyCoordinator` defines atomic claim semantics (`Claim`, `Complete`, `Release`) with leased in-progress locks and request hash validation to detect duplicate payload conflicts.
- Reusing a key for a different payload returns `Conflict`; corrupt responses or missing/corrupt saved request hashes fail closed instead of executing the handler again. Successful responses are replayed only until the configured TTL expires. Handler errors are not cached and may be retried; expired distributed claims may be taken over while the original handler is still running. Use downstream idempotency or fencing when repeating side effects is unsafe.
- Same-process callers share one active execution. Shared work keeps the leader's context values but not its cancellation; coalescing and deduplication use `DefaultSharedFlightTimeout` (2 minutes), while idempotency uses `IdempotencyConfig.FlightTimeout` (default 2 minutes). Each caller can stop waiting on its own cancellation, but the synchronous leader call may remain blocked until the handler returns or the shared timeout expires. `MaxConcurrentFlights` bounds unique idempotency keys (default 256); no background handler goroutines are started.
- This active-flight cap applies to `Idempotency`; `Dedup` and `Coalesce` do not expose a `MaxConcurrentFlights` limit. High-cardinality distinct keys can therefore grow their in-flight state without a configured bound under load; account for this when choosing those middleware for untrusted or bursty traffic.
- Durable and idempotent results are persisted as JSON. For empty-interface result types, including named empty interfaces, the first successful call is normalized to the same standard values returned by JSON replay (numbers are `float64`, objects are `map[string]any`, arrays are `[]any`, and typed nil pointers/maps/slices become nil). Concrete result types use normal `encoding/json` decoding into their declared `Res` type on replay. A non-empty interface generally cannot be reconstructed from JSON as its original concrete dynamic type, so use a concrete result type when replay must succeed; arbitrary dynamic Go types are not restored from persisted type names.
- A `Journal.RecordStep` error or idempotency completion error can be ambiguous: the write may have committed before the error was returned. Durable journals do not guarantee exactly-once external side effects after an ambiguous write failure; use downstream idempotency/fencing or a storage protocol with definitive commit outcomes when repeats are unsafe.

### Durable Execution Identity and Resume
- `DigestBytes` and `DigestString` create fixed-size SHA-256 bindings for execution identity.
- `ExecutionIdentity` binds an execution to its execution namespace, source digest, input digest, and contract digest.
- `DurableWithResume` extends `Durable` with journal-backed identity validation before replay.
- A changed source or input returns an `xerr.Conflict`; callers may use `ResumeOptions{Force: true, ForceReason: ...}` to create an explicit successor generation.
- `ResumeJournal` stores identity metadata alongside the existing `Journal` contract. Durable replay and singleflight remain owned by the shared Kernel runner.

---

## Distributed Coordination and Sagas

### Distributed Locks
- `Exclusive(mutex, ttl, keyFn)` acquires an application-provided `Mutex` before execution and releases it upon completion. Contention returns `ErrLocked`; an empty key is rejected.
- `LeaderOnly(mutex, ttl)` runs the action under a fixed leader key.

### Monotonic Fenced Coordination
- `ExclusiveFenced(fencedMutex, ttl, keyFn)` and `LeaderOnlyFenced` acquire a `LockLease` containing a monotonically increasing fence counter.
- An internal ticker renews the lease at `ttl / 3`. If lease renewal fails or the lease is lost, the action context is canceled immediately to abort in-flight work.
- Handlers retrieve the lease via `action.LeaseFromContext(ctx)`.

For fencing to protect writes, the downstream database or storage operation must validate the token (for example, in the update condition). Kernel manages lease lifecycle and cancellation but cannot make a storage system enforce fencing.

### Sagas
`action.NewSaga[Req, Res]` coordinates distributed operations with compensating transactions:
- `AddStep(name, do, undo)` registers forward work and its reverse compensation.
- `AddOptionalStep(name, do, undo)` registers steps that may fail without triggering a saga abort.
- If a mandatory step fails or panics, `Undo` functions for prior successful steps execute in reverse order under a context detached from parent cancellation and bounded by an independent compensation timeout.
- Each `Undo` receives the original request passed to `Do`, not the result of its forward step.
- `.AsAction()` converts the compiled saga into an executable `*BuiltAction[Req, SagaResult[Res]]`.

---

## Streaming and State Machines

### Streaming
`action.NewStream` wraps `iter.Seq2` handlers under action lifecycle management. `NewStreamOp` composes typed stream actions, while `StreamWorkerPool` adds bounded per-item parallelism:

```go
stream := action.NewStream("telemetry.stream", func(ctx context.Context, req Query) (iter.Seq2[Record, error], error) {
    return func(yield func(Record, error) bool) {
        for i := 0; i < req.Limit; i++ {
            if ctx.Err() != nil {
                yield(Record{}, ctx.Err())
                return
            }
            if !yield(Record{ID: i}, nil) {
                return // Consumer stopped early
            }
        }
    }, nil
})

seq, err := stream.Do(ctx, Query{Limit: 100})
for record, err := range seq {
    // Yield values as the consumer iterates.
}
```

`CollectStream` materializes an iterator into an in-memory slice when streaming is not required. The `stream` package also provides time-based grouping (`BatchByTime`), sliding windows, throttling, debouncing, distinct filtering, and context-aware operators. Use the supplied context; do not substitute `context.Background()` inside an operator.

### State Machines
`action.NewStateMachine` guards entity lifecycle transitions:
- Handlers implement `StateEntity` (`GetState()`, `SetState()`).
- `Allow(from, to...)` registers permissible transitions.
- Disallowed transitions are rejected with `xerr.KindConflict` before committing state changes.

---

## Deterministic DAG Workflows (`ai/dag`)

The `ai/dag` package compiles directed acyclic graphs into deterministic, concurrent execution layers:

```go
graph, err := dag.New("order.workflow").
    AddNode("validate", "val_out", validateAction).
    AddNode("reserve", "res_out", reserveAction).
    AddNode("charge", "chg_out", chargeAction).
    AddEdge("validate", "reserve").
    AddEdge("validate", "charge").
    Compile()
if err != nil {
    return err
}

initialState := dag.AcquireState()
defer initialState.Release()

finalState, err := graph.Execute(ctx, initialState)
if err != nil {
    return err
}
defer finalState.Release()

res, err := dag.GetNodeOutput[ReserveResult](finalState, "reserve")
```

### DAG Characteristics
- **Cycle Detection:** Compiles layers using Kahn's algorithm; circular dependencies return `xerr.KindConflict`.
- **Namespaced Outputs:** Node outputs are stored under `tasks.<node_id>.output` to prevent key collisions during parallel execution.
- **State lifecycle:** Call `state.Release()` when finished with a state value.
- **Sub-graph Nesting:** A compiled DAG can be converted into an action via `.AsAction()` and embedded directly as a node in a parent DAG.
- **Mermaid Export:** `graph.ToMermaid()` exports a deterministic Mermaid flowchart.

### Checkpointing & Human-in-the-Loop (HIL) Resumption

To pause a workflow for an approval or other external step, return `dag.ErrSuspended`; persist the partial state and pass a restored snapshot when resuming:

```go
// 1. Initial execution halts at approval step
state, err := graph.Execute(ctx, initialState)
if errors.Is(err, dag.ErrSuspended) {
    // Partial progress is preserved; persist state snapshot to storage
    db.SaveWorkflow(workflowID, state.Data())
    state.Release()
    return
}

// 2. Later: Resume after human approval
savedSnapshot := db.LoadWorkflow(workflowID)
resumeState := dag.AcquireState()
for k, v := range savedSnapshot { resumeState.Set(k, v) }
defer resumeState.Release()

// Completed node actions are skipped; routing is evaluated again from entry
finalState, err := graph.Execute(ctx, resumeState)
```

- When a node returns `dag.ErrSuspended`, `Execute` returns `(partialState, dag.ErrSuspended)` without purging the state from memory.
- A resumed dynamic DAG starts traversal again at its configured entry. Node actions whose output key (`tasks.<nodeID>.output`) is already present in `State` are skipped, but conditional-edge routers run again on each traversal.
- A router must choose the same target for the same persisted state and stable graph configuration. Do not rely on time, randomness, or mutable external state when a resumed workflow must follow the same branch; a route may change intentionally when the persisted state or graph configuration changes.

---

## Context and Categorized Errors

### Request Scopes (`xctx`)
`xctx` manages request-scoped metadata:
- `NewScope(parent)` obtains a pooled `*RequestScope`. Call the returned `cleanup()` function upon request completion to return the scope to `sync.Pool`.
- Context helpers provide typed accessors: `WithUserID`, `WithTenantID`, `WithRoles`, `WithPermissions`, `WithFeatures`, `WithTraceID`.
- Request-scope setters, accessors, and authorization checks synchronize mutable metadata; direct field access through `ScopeFrom` remains the caller's synchronization responsibility.
- Each accessor acquires a scope lock. For related multi-field reads, `SnapshotFrom(ctx)` takes one read lock and returns a detached identity/authorization snapshot; it clones role, permission, and feature slices (so it may allocate) and intentionally omits the separately locked trace-event ring. Use typed accessors for isolated reads; opt into a snapshot when a coherent multi-field view justifies the copy cost.
- `CloneForAsync(ctx)` clones request metadata into a detached context, preserving tracing and identity while decoupling from the parent cancellation tree.

### Atomic Filesystem Writes (`xfs`)
`WriteFileAtomic` syncs the parent directory after rename on POSIX and returns a sync error even though the rename may already have taken effect. Windows renames remain atomic but cannot be made crash-durable with directory fsync.

### Context checks and audit hooks
- `RequireAuth()` checks for an authenticated context (`xerr.KindUnauthorized`).
- `RequireTenant()` checks for a populated tenant ID.
- `RequireRole(role)` and `RequireAnyRole(roles...)` check role values in context.
- `RequirePermission(perm)` checks permission strings (supports wildcard `*`).
- `RequireFeature(flags...)` and `RequireAnyFeature(flags...)` check feature values.
- `RequireCreationLimit(resource, checkFn)` checks resource quotas on entity creation (`ID == 0`).
- `ImmutableWhen(guardFn, reason)` aborts execution if an entity is in a locked/terminal state.
- `TrackPIIAccess(tracker, purpose)` records a reason for personal-data access.
- `Audited(logger, category, detailsFn)` records audit events upon successful action execution.
- `Transactional(runner)` wraps execution inside an application-provided `TxRunner`.

### Categorized Errors (`xerr`)
`xerr` provides structured error categories to drive consistent transport mapping and retry decisions:

```go
// Categories: BadRequest, Unauthorized, Forbidden, NotFound, Conflict,
// Validation, TooManyRequests, Timeout, Unavailable, Internal, CircuitBreaker.
return xerr.NotFound("user not found")
return xerr.Unavailable("database connection dropped", originalErr)
```

- `xerr.IsTransient(err)` classifies an error for retry policy; it cannot determine whether the operation's side effects are safe to repeat.
- `xerr.From(err)` converts arbitrary Go errors into structured `*AppError` types.
- `appErr.Public(requestID)` returns a client-facing `ErrorResponse` without cause or stack fields. Internal errors use a generic message, and validation detail values are omitted; keep other messages safe for clients.
- `xerr.Sprint(err)` includes the cause chain and resolved stack frames in development (`ENV=dev` or `ENV=development`, or `DEBUG=1`); otherwise it formats the error kind and top-level message.

---

## Observation and Lifecycle Telemetry

The optional `observe` package adapts action hooks to vendor-neutral lifecycle events. In-memory events can include request and response values; the Slog, JSONL, and Prometheus sinks omit payloads. `MemorySink` retains complete events and is intended for tests. Attach a sink through an action hook:

```go
type fanoutSink struct{ sinks []observe.Sink }
func (f *fanoutSink) Emit(ctx context.Context, e observe.Event) {
    for _, s := range f.sinks { s.Emit(ctx, e) }
}

sink := &fanoutSink{sinks: []observe.Sink{
    observe.NewSlogSink(slog.Default()),
    observe.NewMetricsSink(),
    observe.NewPrometheusSink(),
    observe.NewMemorySink(100),
    observe.NewJSONLSink(os.Stdout, 4096),
}}

act := action.New("user.find", findUser).
    AnyHook(observe.Hook(sink)).
    Build()
```

### Event Taxonomy

| Kind | Emitted When | Relevant Event Fields |
| :--- | :--- | :--- |
| `success` | Action completes successfully | `Request`, `Response`, `Duration` |
| `error` | Action fails with an error | `Error`, `Duration` |
| `timeout` | Action returns `context.DeadlineExceeded` | `Request`, `Duration` |
| `retry` | Retry middleware initiates another attempt | `Attempt`, `Error`, `Duration` |
| `cache_hit` | A cache layer serves the response | `Request`, `Response`, `Duration` |
| `cache_miss` | No cache layer contains the key | `Request`, `Duration` |
| `canceled` | Action observes caller context cancellation | `Request`, `Duration` |
| `panic` | Action recovers an unhandled panic | `Recovered`, `Request`, `Duration` |
| `deduplicated` | Concurrent duplicate waits on an existing flight | `Request`, `Duration` |
| `coalesced` | Concurrent caller receives a coalesced result | `Request`, `Duration` |

Every event includes `Time`, `Duration`, `Kind`, `Action`, and available correlation identifiers (`ExecutionID`, `TraceID`, `SpanID`).

- Context cancellations emit `canceled` and are not duplicated as `error` events.
- Recovered panics emit a `panic` event followed by an `error` event containing the resulting `xerr.KindInternal` error.

### Built-in Sinks
- `MemorySink`: Stores events in a bounded, thread-safe in-memory ring buffer. `Events()` returns an isolated slice snapshot in oldest-to-newest order.
- `MetricsSink`: Aggregates counters by action name and event kind. `Snapshot()` returns an isolated map.
- `PrometheusSink`: Generates deterministic Prometheus exposition text with escaped labels.
- `JSONLSink`: Writes structured JSON Lines event metadata, including request/response type names but not payloads; raw errors and recovered panic values are replaced with `[redacted]`.
- `SlogSink`: Dispatches structured logs including duration through standard library `log/slog`.

## Examples Reference

Complete, runnable examples are maintained in the central [`nexssp/examples`](https://github.com/nexssp/examples/tree/main/kernel) repository. This repository keeps only [`examples/README.md`](./examples/README.md) as a pointer:

- [`01_ai_dag_pipeline`](https://github.com/nexssp/examples/tree/main/kernel/01_ai_dag_pipeline) – Multi-layer parallel DAG execution with typed outputs.
- [`02_resilient_action`](https://github.com/nexssp/examples/tree/main/kernel/02_resilient_action) – Deduplication, multi-layer caching, exponential jitter retry, and hooks.
- [`03_composition_and_fanout`](https://github.com/nexssp/examples/tree/main/kernel/03_composition_and_fanout) – Sequential pipelines (`Pipe`) and bounded concurrency (`FanOut`).
- [`04_fenced_distributed_lock_failover`](https://github.com/nexssp/examples/tree/main/kernel/04_fenced_distributed_lock_failover) – Monotonic fencing token renewal and lease-loss cancellation.
- [`05_parallel_scatter_gather_recovery`](https://github.com/nexssp/examples/tree/main/kernel/05_parallel_scatter_gather_recovery) – Transient error recovery during parallel fan-out.
- [`06_pure_streaming_pipeline`](https://github.com/nexssp/examples/tree/main/kernel/06_pure_streaming_pipeline) – Lazy processing using `iter.Seq2`.
- [`07_hierarchical_nested_dag`](https://github.com/nexssp/examples/tree/main/kernel/07_hierarchical_nested_dag) – Embedding compiled DAGs as executable nodes within parent DAGs.
- [`08_thundering_herd_coalescing`](https://github.com/nexssp/examples/tree/main/kernel/08_thundering_herd_coalescing) – Request coalescing.
- [`09_retry_policies`](https://github.com/nexssp/examples/tree/main/kernel/09_retry_policies) – Custom predicates, transient-only retries, and universal retry policies.
- [`10_observe_lifecycle`](https://github.com/nexssp/examples/tree/main/kernel/10_observe_lifecycle) – End-to-end lifecycle observation with Prometheus, Slog, JSONL, and Metrics sinks.
- [`11_lazy_etl_stream`](https://github.com/nexssp/examples/tree/main/kernel/11_lazy_etl_stream) – ETL over typed streams.
- [`12_stream_action_crossover`](https://github.com/nexssp/examples/tree/main/kernel/12_stream_action_crossover) – Crossing typed actions and streams.
- [`13_resilient_mod_host`](https://github.com/nexssp/examples/tree/main/kernel/13_resilient_mod_host) – Host-owned resilience and module integration.
