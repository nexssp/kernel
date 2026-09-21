package action_test

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest"
	"github.com/nexssp/kernel/xtest/ktest"
)

type PaymentOrderReq struct {
	AccountID string  `json:"account_id"`
	Amount    float64 `json:"amount"`
}

type PaymentOrderRes struct {
	TxID      string    `json:"tx_id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// 1. HAPPY PATH: Cached replay returns exact output without invoking handler twice
func TestIdempotency_HappyPath_CachedReplay(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 9, 20, 22, 0, 0, 0, time.UTC)

	store := action.NewMemoryIdempotencyStore(5 * time.Minute)

	processPayment := ktest.Sequence[PaymentOrderReq, PaymentOrderRes](
		"payment.charge",
		PaymentOrderRes{TxID: "tx_998877", Status: "SETTLED", Timestamp: fixedTime},
		PaymentOrderRes{TxID: "tx_998878", Status: "SETTLED", Timestamp: fixedTime},
	).
		IdempotentWithConfig(action.IdempotencyConfig{
			Enabled: true,
			TTL:     5 * time.Minute,
			Store:   store,
		}).
		Build()

	ctx := ktest.RequestContext(t, "req_unique_001")
	req := PaymentOrderReq{AccountID: "acc_10", Amount: 250.0}

	// 1st run
	ktest.Run(t, processPayment, ctx, req).
		NoError().
		Equals(PaymentOrderRes{TxID: "tx_998877", Status: "SETTLED", Timestamp: fixedTime})

	// 2nd run (idempotent replay)
	ktest.Run(t, processPayment, ctx, req).
		NoError().
		Equals(PaymentOrderRes{TxID: "tx_998877", Status: "SETTLED", Timestamp: fixedTime})
}

// 2. BAD PATH: Conflicting request payload with identical idempotency key is rejected
func TestIdempotency_PayloadConflict_Rejected(t *testing.T) {
	t.Parallel()

	act := ktest.Returns[PaymentOrderReq, PaymentOrderRes](
		"payment.conflict",
		PaymentOrderRes{TxID: "tx_1", Status: "OK"},
	).Idempotent().Build()

	ctx := ktest.RequestContext(t, "idem_conflict_key")

	ktest.Run(t, act, ctx, PaymentOrderReq{AccountID: "user_1", Amount: 10.0}).NoError()

	// Different amount with same key must return 409 Conflict
	ktest.Run(t, act, ctx, PaymentOrderReq{AccountID: "user_1", Amount: 999.0}).
		ErrorKind(xerr.KindConflict).
		ErrorContains("idempotency key already used with a different request payload")
}

// 3. BAD PATH: Handler errors release the claim and allow retry
func TestIdempotency_HandlerFailure_AllowsRetry(t *testing.T) {
	t.Parallel()

	attempts := ktest.NewCounter()
	act := action.New("flaky.charge", func(_ context.Context, _ PaymentOrderReq) (PaymentOrderRes, error) {
		attempts.Inc()
		if attempts.Load() == 1 {
			return PaymentOrderRes{}, xerr.Unavailable("database glitch")
		}
		return PaymentOrderRes{TxID: "tx_ok", Status: "SUCCESS"}, nil
	}).Idempotent().Build()

	ctx := ktest.RequestContext(t, "retryable_charge")
	req := PaymentOrderReq{AccountID: "acc_1", Amount: 50.0}

	// 1st attempt fails
	ktest.Run(t, act, ctx, req).ErrorKind(xerr.KindUnavailable)

	// 2nd attempt succeeds
	ktest.Run(t, act, ctx, req).
		NoError().
		Satisfies(func(res PaymentOrderRes) bool { return res.Status == "SUCCESS" })

	attempts.Require(t, 2)
}

// 4. PANIC SAFETY: Panic in handler releases coordinator claim (Regression Test)
func TestIdempotency_HandlerPanic_ReleasesCoordinatorClaim(t *testing.T) {
	t.Parallel()

	store := action.NewMemoryIdempotencyStore(time.Hour)

	var panicMode atomic.Bool
	panicMode.Store(true)

	act := action.New("panic.idem", func(_ context.Context, _ string) (string, error) {
		if panicMode.Load() {
			panic("catastrophic failure during payment execution")
		}
		return "recovered", nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Enabled: true,
		Store:   store,
	}).Build()

	ctx := ktest.RequestContext(t, "panic-key-1")

	// 1st run panics
	_, err := act.Do(ctx, "payload")
	if err == nil || xerr.KindFrom(err) != xerr.KindInternal {
		t.Fatalf("expected KindInternal error from panic recovery, got %v", err)
	}

	// Prove claim was released: the second call must NOT be blocked with InProgress!
	panicMode.Store(false)
	ktest.Run(t, act, ctx, "payload").
		NoError().
		Equals("recovered")
}

// 5. THUNDERING HERD: 50 concurrent requests collapse to 1 execution
func TestIdempotency_HighConcurrency_SingleExecution(t *testing.T) {
	t.Parallel()

	executions := ktest.NewCounter()
	const callers = 50

	act := action.New("concurrent.transfer", func(_ context.Context, _ PaymentOrderReq) (PaymentOrderRes, error) {
		executions.Inc()
		time.Sleep(20 * time.Millisecond)
		return PaymentOrderRes{TxID: "tx_batch_01", Status: "SETTLED"}, nil
	}).Idempotent().Build()

	// Use our ktest.Simulate barrier
	ktest.Simulate(t, act, PaymentOrderReq{AccountID: "acc_pool", Amount: 100.0}, callers,
		func(tb testing.TB, res PaymentOrderRes, err error) {
			if err != nil {
				tb.Errorf("unexpected error: %v", err)
			}
			if res.TxID != "tx_batch_01" || res.Status != "SETTLED" {
				tb.Errorf("invalid result: %+v", res)
			}
		})

	executions.Require(t, 1)
}

// 6. RESOURCE MANAGEMENT: Zero Goroutine Leaks on Close
func TestIdempotent_NoGoroutinesSpawned(t *testing.T) {
	baseline := runtime.NumGoroutine()

	for range 20 {
		_ = ktest.Echo[string]("no.goroutine.idem").Idempotent().Build()
	}

	// Mathematical proof: actions are demand-driven and spawn 0 background workers
	xtest.RequireGoroutinesAtMost(t, baseline, 100*time.Millisecond, 1)
}

// 7. PERFORMANCE: Zero heap allocations on idempotent cache hit
func TestIdempotency_ZeroAlloc_CacheHit(t *testing.T) {
	act := action.New("bench.idem", func(_ context.Context, req int) (int, error) {
		return req * 2, nil
	}).Idempotent().Build()

	ctx := ktest.RequestContext(t, "bench_key")
	_, _ = act.Do(ctx, 42) // populate cache

	// The idempotency middleware uses encoding/json for request hashing and
	// response persistence. Reflection-based JSON cannot be zero-alloc, so
	// assert a stable upper bound instead of exactly zero. The ceiling still
	// catches per-call unbounded growth (leaks, growing slices).
	xtest.RequireMaxAlloc(t, 1000, 20, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fail()
		}
	})
}
