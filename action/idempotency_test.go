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

type idempotencyGetBarrier struct {
	action.IdempotencyStore
	gets  atomic.Int32
	want  int32
	ready chan struct{}
}

func (s *idempotencyGetBarrier) Get(ctx context.Context, key string) (action.IdempotencyEntry, bool) {
	entry, ok := s.IdempotencyStore.Get(ctx, key)
	if s.gets.Add(1) == s.want {
		close(s.ready)
	}
	return entry, ok
}

func TestIdempotency_UnhashableRequestFailsClosed(t *testing.T) {
	type unhashableRequest struct {
		Callback func()
	}

	var calls atomic.Int32
	act := action.New("idempotency.unhashable", func(context.Context, unhashableRequest) (string, error) {
		calls.Add(1)
		return "unexpected", nil
	}).Idempotent().Build()

	contexts := []context.Context{
		context.Background(),
		ktest.RequestContext(t, "unhashable-request-key"),
	}
	for _, ctx := range contexts {
		_, err := act.Do(ctx, unhashableRequest{Callback: func() {}})
		if err == nil || xerr.KindFrom(err) != xerr.KindBadRequest {
			t.Fatalf("unhashable request error = %v, want BadRequest", err)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
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
	store := &idempotencyGetBarrier{
		IdempotencyStore: action.NewMemoryIdempotencyStore(time.Hour),
		want:             int32(callers + 1),
		ready:            make(chan struct{}),
	}
	release := make(chan struct{})

	act := action.New("concurrent.transfer", func(context.Context, PaymentOrderReq) (PaymentOrderRes, error) {
		executions.Inc()
		<-release
		return PaymentOrderRes{TxID: "tx_batch_01", Status: "SETTLED"}, nil
	}).IdempotentWithConfig(action.IdempotencyConfig{
		Enabled: true,
		Store:   store,
	}).Build()

	type result struct {
		value PaymentOrderRes
		err   error
	}
	results := make(chan result, callers)
	start := make(chan struct{})
	ctx := ktest.RequestContext(t, "thundering-herd")
	request := PaymentOrderReq{AccountID: "acc_pool", Amount: 100.0}
	for range callers {
		go func() {
			<-start
			value, err := act.Do(ctx, request)
			results <- result{value: value, err: err}
		}()
	}
	close(start)

	timedOut := false
	select {
	case <-store.ready:
	case <-time.After(5 * time.Second):
		timedOut = true
	}
	close(release)

	for range callers {
		got := <-results
		if got.err != nil {
			t.Errorf("unexpected error: %v", got.err)
		}
		if got.value.TxID != "tx_batch_01" || got.value.Status != "SETTLED" {
			t.Errorf("invalid result: %+v", got.value)
		}
	}
	if timedOut {
		t.Fatal("timed out waiting for all concurrent idempotency lookups")
	}

	executions.Require(t, 1)
}

// 6. RESOURCE MANAGEMENT: Bound goroutine growth during construction.
func TestIdempotent_BuildKeepsGoroutineCountBounded(t *testing.T) {
	baseline := runtime.NumGoroutine()

	for range 20 {
		_ = ktest.Echo[string]("no.goroutine.idem").Idempotent().Build()
	}

	// One extra goroutine is allowed for transient testing-runtime activity.
	xtest.RequireGoroutinesAtMost(t, baseline, 100*time.Millisecond, 1)
}

// 7. PERFORMANCE: Keep the measured cached-replay allocation budget explicit.
func TestIdempotency_CacheHitAllocationBudget(t *testing.T) {
	act := action.New("bench.idem", func(_ context.Context, req int) (int, error) {
		return req * 2, nil
	}).Idempotent().Build()

	ctx := ktest.RequestContext(t, "bench_key")
	_, _ = act.Do(ctx, 42) // populate cache

	// The cache-hit benchmark measures one allocation per call.
	xtest.RequireMaxAlloc(t, 1000, 1, func() {
		res, err := act.Do(ctx, 42)
		if err != nil || res != 84 {
			t.Fail()
		}
	})
}
