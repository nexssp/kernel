# Nexss Kernel

Nexss Kernel is a lightweight, typed Go execution library for building reusable actions, resilient pipelines, and deterministic DAG workflows. It is deliberately independent of concrete transport protocols, databases, AI vendor SDKs, and specific identity providers.

The Kernel provides the core execution semantics, typed generics, context-based security guards, and resilient middleware. Applications and sibling modules provide concrete network adapters, persistence implementations, and domain logic.

> **Design goal:** predictable execution semantics, focused interfaces, zero-allocation hot paths, and idiomatic Go.

---

## Architecture

```text
Application / Domain Package
        │
        ├── Domain builders (appaction pattern)
        ├── HTTP / CLI / Worker / NATS adapters
        ├── AI, Tooling & Provider modules
        └── Database, Cache & Storage implementations
        │
        ▼
Nexss Kernel
  typed actions · Builder · middleware · hooks · retry
  cache/idempotency contracts · context guards (RBAC/perms)
  composition · DAG graphs · categorized errors (xerr)
```

The Kernel owns the lifecycle and semantics of an action execution. Extension modules implement backend drivers or add explicit middleware layers; they do not fork the builder or change execution semantics.

---

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/nexssp/kernel/action"
)

type CreateOrderRequest struct {
    CustomerID string `json:"customer_id"`
    SKU        string `json:"sku"`
    Quantity   int    `json:"quantity"`
}

type Order struct {
    ID string `json:"id"`
}

func main() {
    createOrder := action.New("order.create", func(ctx context.Context, req CreateOrderRequest) (Order, error) {
        // Business logic execution
        return Order{ID: req.CustomerID + ":" + req.SKU}, nil
    }).
        Timeout(3 * time.Second).
        Retry(2, action.ExponentialJitter(50*time.Millisecond, time.Second)).
        Idempotent().
        Build()

    order, err := createOrder.Do(context.Background(), CreateOrderRequest{
        CustomerID: "customer-42",
        SKU:        "sku-100",
        Quantity:   2,
    })
    if err != nil {
        panic(err)
    }

    fmt.Println("Order created:", order.ID)
}
```

---

## Key Features

### 1. Typed Execution & Generic Inference
`action.New` automatically infers request and response types from the handler signature without reflection on the hot path:

```go
act := action.New("user.get", getUser).
    Timeout(2 * time.Second).
    Retry(2, action.ExponentialJitter(10*time.Millisecond, 500*time.Millisecond)).
    Build()

user, err := act.Do(ctx, GetUserRequest{ID: "user-42"})
```

### 2. Context Security Guards
The Kernel provides typed context guards (RBAC, permissions, feature flags, tenant checks, and rate limits) that evaluate against `context.Context` without baking in any specific identity provider (Auth0, JWT, OAuth, or custom sessions):

```go
act := action.New("invoice.delete", deleteInvoice).
    RequireAuth().
    RequireRole("admin").
    RequirePermission("invoices:delete").
    RequireFeature("billing-v2").
    Build()
```

### 3. Resilience & Protection Pipeline
* **Retry:** Exponential backoff with random jitter.
* **Circuit Breaker:** Adaptive failure threshold and half-open state transitions.
* **Deduplication:** Singleflight deduplication for concurrent identical in-flight requests.
* **Cache:** Multi-layer L1/L2 read-through caching with automated back-fill.
* **Bulkhead:** Concurrency and rate limiting.

### 4. Deterministic DAG Execution (`ai/dag`)
Compose independent actions into directed acyclic graphs. The engine validates cycle freedom, compiles deterministic layers, executes concurrent nodes via `errgroup`, and merges output state safely:

```go
graph, err := dag.New("order_pipeline").
    AddNode("user_node", "user", fetchUserAction).
    AddNode("stock_node", "stock", checkStockAction).
    AddNode("charge_node", "charge", chargePaymentAction).
    AddEdge("user_node", "charge_node").
    AddEdge("stock_node", "charge_node").
    Compile()

finalState, err := graph.Execute(ctx, initialState)
```

---

## Examples

Explore runnable examples in the [`examples/`](./examples) directory:
* [`01_ai_dag_pipeline`](./examples/01_ai_dag_pipeline) – Multi-layer parallel DAG graph with typed state outputs.
* [`02_resilient_action`](./examples/02_resilient_action) – Cache + Deduplication + Jitter Retry + Circuit Breaker.
* [`03_composition_and_fanout`](./examples/03_composition_and_fanout) – Action Pipelines (`Pipe`) and bounded parallel `FanOut`.
* [`04_fenced_distributed_lock_failover`](./examples/04_fenced_distributed_lock_failover) – Split-brain protection with fenced monotonic locks.
* [`05_parallel_scatter_gather_recovery`](./examples/05_parallel_scatter_gather_recovery) – Selective transient error recovery in parallel fan-out.
* [`06_pure_streaming_pipeline`](./examples/06_pure_streaming_pipeline) – Go 1.23+ `iter.Seq2` constant O(1) memory stream processing.
* [`07_hierarchical_nested_dag`](./examples/07_hierarchical_nested_dag) – Composing DAG graphs as executable nodes within parent DAGs.
* [`08_thundering_herd_coalescing`](./examples/08_thundering_herd_coalescing) – Request coalescing and singleflight leader execution.

---

## Performance Model

The Kernel is designed for low overhead:
* **Zero reflection on the hot path:** Normal execution uses direct generic function calls.
* **Immutable post-build state:** Built actions are safe for concurrent reuse across goroutines.
* **Composed middleware:** Chained at `Build()` time rather than evaluated dynamically per invocation.
* **Sub-microsecond overhead:** Core action dispatch overhead is typically `< 50 ns/op` and 0 allocations on in-memory paths.

---

## Testing & Verification

Run the test suite, race detector, and benchmarks:

```bash
go test ./...
go test -race ./...
go vet ./...
go test -bench=. -benchmem ./action/...
```

---

## Contributing

Read [`CONTRIBUTING.md`](./CONTRIBUTING.md) before opening a pull request. Keep changes small, typed, testable, and measured.

## Security

Read [`SECURITY.md`](./SECURITY.md) for vulnerability reporting procedures.

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
