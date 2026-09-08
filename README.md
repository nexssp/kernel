# Nexss Kernel

Nexss Kernel is a lightweight, typed Go execution library for building reusable actions, resilient pipelines, and deterministic DAG workflows. It is independent of concrete transport protocols, persistence implementations, AI vendor SDKs, and specific identity providers.

The Kernel provides core execution semantics, typed generics, context-based security guards, and resilient middleware. Applications and sibling modules supply concrete network adapters, storage drivers, and domain logic.

> **Design goal:** Predictable execution semantics, focused interfaces, zero-allocation hot paths, and idiomatic Go.

---

## Architecture

```text
Application / Domain Package / Transport Adapters
        │
        ├── Domain builders (appaction pattern)
        ├── HTTP / CLI / Worker / NATS / gRPC adapters
        ├── AI, Tooling & Provider modules
        └── Database, Cache & Storage implementations
        │
        ▼
Nexss Kernel
  typed actions · Builder · middleware · hooks · retry
  cache/idempotency contracts · context guards (RBAC/perms)
  composition · DAG graphs · categorized errors (xerr)
```

The Kernel owns the lifecycle, concurrency safety, and execution semantics of an action. Extension modules implement backend drivers or add explicit middleware layers; they do not fork the builder or alter execution semantics.

---

## Core Packages

| Package | Purpose |
| :--- | :--- |
| `action` | Typed action builders, execution, hooks, middleware, composition, retries, timeouts, concurrency limits, caching, idempotency, deduplication, coalescing, streaming, sagas, and resilience policies. |
| `observe` | Optional vendor-neutral lifecycle events, bounded memory history, metrics aggregation, Prometheus, JSONL, and `slog` sinks. |
| `ai/dag` | Typed directed acyclic graphs with deterministic compilation, layered parallel execution, nested graphs, state snapshots, Mermaid rendering, and durable checkpoint/resume (HIL) semantics. |
| `xctx` | Typed request context values, request scopes, correlation data, trace data, roles, permissions, features, and scope pooling. |
| `xerr` | Categorized errors for validation, authorization, conflicts, not-found cases, transient failures, internal failures, and panic recovery. |

---

## Basic Action Usage

Actions are typed generic functions wrapped with fluent middleware pipelines. Request and response types are inferred from the handler signature without reflection on the hot path:

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

## Dynamic Dispatch and Reflection-Free Bridge

The Kernel supports type-erased and dynamic execution boundaries (such as AI tool-calling agents, plugin registries, and generic HTTP routers) without weakening typed internal handlers:

```go
act := action.New("user.lookup", lookupHandler).Build()

// 1. Direct type-erased call: 0 heap allocations on matching Req or *Req pointer
result, err := act.DoAny(ctx, UserRequest{ID: "usr-42"})

// 2. Universal Invoker: automatically decodes and coerces dynamic payloads
result, err = action.InvokeAny(ctx, act, map[string]any{"id": "usr-42"})

// 3. Coerce: converts dynamic structures (JSON, maps, strings) into target types
req, err := action.Coerce[UserRequest](untypedInput)

// 4. Dynamic lifting: lifts AnyAction into *Builder[any, any] for composition
dynamicAct := action.Dynamic(act).Build()
```

---

## Composition and Pipelines

- `Pipe[Req, Mid, Res]` connects two actions sequentially (`A → B`).
- `PipeWith[Req, Mid1, Mid2, Res]` connects two actions with an inline transformation function, avoiding intermediate adapter structs.
- `Chain[T]` executes same-typed actions sequentially, piping output to input.
- `Branch` and `BranchAny` route execution conditionally using a router function.
- `FirstSuccess` runs fallback chains, returning the first non-error result.
- `Parallel` executes typed actions concurrently over the same request (scatter-gather).
- `ParallelNamed` and `ParallelMap` execute heterogeneous actions concurrently, collecting results into a `map[string]any`.

```go
// Pipeline with zero-allocation inline field transform
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

## Resilience, Concurrency, and Traffic Protection

### Retries and Circuit Breaking
- `Retry` retries only transient errors identified by `xerr.IsTransient`.
- `RetryIf` evaluates an application-defined `RetryPredicate(err error) bool`.
- `RetryAll` retries on any non-nil error (intended for idempotent batch jobs).
- `InferredResilient` / `SmartResilience` configures an adaptive circuit breaker and exponential jitter retry driven automatically by `xerr.Kind`. It fast-fails 4xx errors (`BadRequest`, `Unauthorized`, `Forbidden`, `NotFound`, `Validation`) without consuming retries.
- `Adaptive` applies a circuit breaker with dynamic timeout scaling and half-open state recovery.

### Backoff Strategies
- `ExponentialBackoff(base, max)` doubles wait duration per attempt.
- `ExponentialJitter(base, max)` adds ±30% random jitter to mitigate thundering-herd spikes.
- `LinearBackoff(step)` and `ConstantBackoff(duration)`.

### Concurrency and Traffic Shields
- `Dedup(keyFn)` applies singleflight deduplication. Callers sharing a key block until active execution completes, receiving copies of the result. Context cancellation of a joined caller does not cancel the flight.
- `Coalesce(coalescer, keyFn)` shares in-flight results across actions. A canceled waiter may exit without terminating the background execution for other callers.
- `ConcurrencyLimit(limit)` enforces an in-process ceiling using atomic counters. Excess calls return `ErrConcurrencyLimit`.
- `RateLimit`, `RateLimitWithKey`, and `RateLimitDistributed` provide token-bucket admission control.
- `AdaptiveLoadShedding` evaluates system statistics (CPU percentage and goroutine count) against priority tiers (`PriorityCritical`, `PriorityNormal`, `PriorityLow`), dropping non-essential traffic when thresholds are exceeded.

### Execution Primitives
- `FanOut(ctx, act, reqs, maxConcurrency)` executes concurrent items while preserving input order in the returned slice. A non-positive concurrency limit executes with unbounded concurrency.
- `Race(ctx, act, reqs)` returns the first successful result and cancels losing attempts. Handlers must cooperate with context cancellation.

---

## Caching and Idempotency

### Multi-Layer Read-Through Caching
`Cache(ttl, keyFn, layers...)` coordinates L1 (local memory) through LN (distributed) stores:
- Cache hits return without invoking the underlying handler.
- Cache misses execute the handler under singleflight protection to prevent cache stampedes.
- Slower-layer hits automatically back-fill faster layers.
- If no layers are passed, an internal thread-safe in-memory cache with an automated background janitor is instantiated. Call `action.Close()` on application shutdown to release janitor goroutines.
- `Once()` memoizes the result or error of the first execution indefinitely for static configurations.

### Idempotency Contracts
- `.Idempotent()` and `.IdempotentWithConfig(cfg)` register idempotency metadata.
- `MemoryIdempotencyStore` provides in-memory reference coordination.
- `IdempotencyCoordinator` defines atomic claim semantics (`Claim`, `Complete`, `Release`) with leased in-progress locks and request hash validation to detect duplicate payload conflicts.

---

## Distributed Coordination and Sagas

### Distributed Locks
- `Exclusive(mutex, ttl, keyFn)` acquires an application-provided `Mutex` before execution and releases it upon completion. Contention returns `ErrLocked`.
- `LeaderOnly(mutex, ttl)` runs the action under a fixed leader key.

### Monotonic Fenced Coordination
- `ExclusiveFenced(fencedMutex, ttl, keyFn)` and `LeaderOnlyFenced` acquire a `LockLease` containing a monotonically increasing fence counter.
- An internal ticker renews the lease at `ttl / 3`. If lease renewal fails or the lease is lost, the action context is canceled immediately to abort in-flight work.
- Handlers retrieve the lease via `action.LeaseFromContext(ctx)`.

> **Contract Notice:** Fencing is only effective if the downstream database or storage operation validates the token (e.g., `UPDATE ... WHERE fence >= @fence`). The Kernel coordinates lease lifecycle and cancellation; it cannot enforce fencing on storage engines that do not verify the token.

### Sagas
`action.NewSaga[Req, Res]` coordinates distributed operations with compensating transactions:
- `AddStep(name, do, undo)` registers forward work and its reverse compensation.
- `AddOptionalStep(name, do, undo)` registers steps that may fail without triggering a saga abort.
- If a mandatory step fails or panics, registered `Undo` functions execute in reverse order (LIFO) under an uncancelled context.
- `.AsAction()` converts the compiled saga into an executable `*BuiltAction[Req, SagaResult[Res]]`.

---

## Streaming and State Machines

### Pure O(1) Memory Streaming
`action.NewStream` wraps Go 1.23+ `iter.Seq2` handlers under action lifecycle management:

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
    // Zero heap allocation streaming
}
```

`CollectStream` materializes an iterator into an in-memory slice when streaming is not required.

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
- **Pooled State:** `State` instances utilize `sync.Pool`. Read operations during layer execution are zero-lock. Callers must call `state.Release()` when finished.
- **Sub-graph Nesting:** A compiled DAG can be converted into an action via `.AsAction()` and embedded directly as a node in a parent DAG.
- **Mermaid Export:** `graph.ToMermaid()` exports deterministic, formatted Mermaid flowchart diagrams.

### Checkpointing & Human-in-the-Loop (HIL) Resumption

Workflows that require asynchronous human intervention (approvals, manual reviews, external callbacks) are supported natively via `dag.ErrSuspended`:

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

// Completed nodes are skipped automatically; execution continues
finalState, err := graph.Execute(ctx, resumeState)
```

- When a node returns `dag.ErrSuspended`, `Execute` returns `(partialState, dag.ErrSuspended)` without purging the state from memory.
- When re-executing with an existing state, any node whose output key (`tasks.<nodeID>.output`) is already present in `State` is skipped, ensuring non-idempotent upstream actions do not re-run.

---

## Context, Security, and Categorized Errors

### Request Scopes (`xctx`)
`xctx` manages request-scoped metadata without heap allocations on standard paths:
- `NewScope(parent)` obtains a pooled `*RequestScope`. Call the returned `cleanup()` function upon request completion to return the scope to `sync.Pool`.
- Context helpers provide typed accessors: `WithUserID`, `WithTenantID`, `WithRoles`, `WithPermissions`, `WithFeatures`, `WithTraceID`.
- `CloneForAsync(ctx)` clones request metadata into a detached context, preserving tracing and identity while decoupling from the parent cancellation tree.

### Security and Governance Guards
- `RequireAuth()` rejects unauthenticated contexts (`xerr.KindUnauthorized`).
- `RequireTenant()` requires a populated tenant ID.
- `RequireRole(role)` and `RequireAnyRole(roles...)` enforce RBAC claims.
- `RequirePermission(perm)` verifies granular permission strings (supports wildcard `*`).
- `RequireFeature(flags...)` and `RequireAnyFeature(flags...)` evaluate feature toggles.
- `RequireCreationLimit(resource, checkFn)` checks resource quotas on entity creation (`ID == 0`).
- `ImmutableWhen(guardFn, reason)` aborts execution if an entity is in a locked/terminal state.
- `TrackPIIAccess(tracker, purpose)` records personal data access reasons for regulatory compliance.
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

- `xerr.IsTransient(err)` checks whether an error is safe to retry.
- `xerr.From(err)` converts arbitrary Go errors into structured `*AppError` types.
- `appErr.Public(requestID)` returns a sanitized `ErrorResponse` suitable for API responses without leaking internal causes or stack traces.
- `xerr.Sprint(err)` renders error trees with sanitized user stack traces in development (`ENV=dev`) and structured one-line outputs in production.

---

## Observation and Lifecycle Telemetry

The optional `observe` package emits vendor-neutral lifecycle events. Sinks are concurrency-safe and receive structured `observe.Event` records with action `Duration` and complete `Request`/`Response` payload references for latency and token/cost attribution:

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
| `executed` | Action completes successfully | `Request`, `Response`, `Duration` |
| `error` | Action fails with an error | `Error`, `Duration` |
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
- Nil hooks and no-op sinks evaluate with zero heap allocations on the hot path.

### Built-in Sinks
- `MemorySink`: Thread-safe circular ring buffer. `Events()` returns an isolated snapshot copy in oldest-to-newest order.
- `MetricsSink`: Aggregates counters by action name and event kind. `Snapshot()` returns an isolated map.
- `PrometheusSink`: Generates deterministic Prometheus exposition text with escaped labels.
- `JSONLSink`: Synchronized, bounded structured JSON writer with serialized durations. Error strings are capped at `maxBytes`.
- `SlogSink`: Dispatches structured logs including duration through standard library `log/slog`.

---

## Architectural Boundaries

The Kernel provides execution semantics and safety contracts; applications supply external integrations:

- **Storage & State:** Cache backends, distributed locks, idempotency stores, and database transactions are supplied via interfaces. The Kernel does not bundle concrete database drivers.
- **Telemetry Ingestion:** `observe` captures lifecycle events and execution duration locally. Forwarding events to OpenTelemetry collectors, Datadog, or external brokers belongs to application sinks.
- **Human-in-the-Loop & Workflows:** The Kernel manages suspension (`dag.ErrSuspended`), partial state preservation, and memoized resumption. Applications supply the human interfaces (webhooks, email dispatch, approval dashboards) and durable persistence for state snapshots.
- **Security Decisions:** Context guards inspect metadata attached to `context.Context`. Authentication (decoding JWTs, session lookups, certificate validation) is handled at transport boundaries.
- **Resource Ownership:** Pooled objects (`dag.State`, `xctx.RequestScope`) must be explicitly released by the caller to prevent resource leaks.

---

## Performance Model and Verification

The Kernel avoids reflection on execution paths, relies on immutable structures post-build, and compiles middleware chains at initialization time.

### Benchmark and Quality Verification

Run formatting checks, static analysis, race-detection tests, and allocation benchmarks:

```bash
# Verify formatting
gofmt -l .

# Run static analysis
go vet ./...

# Run test suite with race detector
go test -race ./...

# Benchmark core dispatch and allocation profiles
go test ./... -run '^$' -bench=. -benchmem
```

---

## Examples Reference

Complete, runnable examples are located in the [`examples/`](./examples) directory:

- [`01_ai_dag_pipeline`](./examples/01_ai_dag_pipeline) – Multi-layer parallel DAG execution with typed outputs.
- [`02_resilient_action`](./examples/02_resilient_action) – Deduplication, multi-layer caching, exponential jitter retry, and hooks.
- [`03_composition_and_fanout`](./examples/03_composition_and_fanout) – Sequential pipelines (`Pipe`) and bounded concurrency (`FanOut`).
- [`04_fenced_distributed_lock_failover`](./examples/04_fenced_distributed_lock_failover) – Monotonic fencing token renewal and lease-loss cancellation.
- [`05_parallel_scatter_gather_recovery`](./examples/05_parallel_scatter_gather_recovery) – Transient error recovery during parallel fan-out.
- [`06_pure_streaming_pipeline`](./examples/06_pure_streaming_pipeline) – Constant O(1) memory processing using Go 1.23+ `iter.Seq2`.
- [`07_hierarchical_nested_dag`](./examples/07_hierarchical_nested_dag) – Embedding compiled DAGs as executable nodes within parent DAGs.
- [`08_thundering_herd_coalescing`](./examples/08_thundering_herd_coalescing) – Request coalescing under high concurrent load.
- [`09_retry_policies`](./examples/09_retry_policies) – Custom predicates, transient-only retries, and universal retry policies.
- [`10_observe_lifecycle`](./examples/10_observe_lifecycle) – End-to-end lifecycle observation with Prometheus, Slog, JSONL, and Metrics sinks.

---

## Contributing

Read [`CONTRIBUTING.md`](./CONTRIBUTING.md) before opening a pull request. Changes must be typed, covered by tests, and include benchmarks for performance-critical paths.

## Security

Report suspected vulnerabilities privately according to [`SECURITY.md`](./SECURITY.md).

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
Copyright © 2018–2026 Marcin Polak and Contributors.
