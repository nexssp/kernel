package xtest

import (
	"errors"
	"testing"
	"time"
)

func TestEventually_ImmediateTrue(t *testing.T) {
	calls := 0
	Eventually(t, time.Second, func() bool {
		calls++
		return true
	})
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestEventually_RetriesUntilTrue(t *testing.T) {
	calls := 0
	Eventually(t, time.Second, func() bool {
		calls++
		return calls >= 4
	})
	if calls != 4 {
		t.Fatalf("calls = %d, want 4", calls)
	}
}

func TestEventually_FailsOnTimeout(t *testing.T) {
	ExpectFatal(t, "TestEventually_FailsOnTimeout", "XTEST_EVENTUALLY_TIMEOUT",
		"condition not met",
		func(t *testing.T) {
			Eventually(t, 15*time.Millisecond, func() bool { return false })
		})
}

func TestEventually_FailsOnNilCond(t *testing.T) {
	ExpectFatal(t, "TestEventually_FailsOnNilCond", "XTEST_EVENTUALLY_NIL",
		"nil condition",
		func(t *testing.T) {
			Eventually(t, time.Second, nil)
		})
}

func TestEventuallyEvery_UsesCustomInterval(t *testing.T) {
	start := time.Now()
	calls := 0
	EventuallyEvery(t, time.Second, 10*time.Millisecond, func() bool {
		calls++
		return calls >= 3
	})
	// 3 calls = 1 immediate + 2 with interval between them.
	// Wall clock must exceed one interval but stay well under the timeout.
	elapsed := time.Since(start)
	if elapsed < 10*time.Millisecond {
		t.Fatalf("elapsed %s: interval not honored", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed %s: interval too long", elapsed)
	}
}

func TestEventuallyEvery_NonPositiveIntervalUsesDefault(t *testing.T) {
	// Must not spin, must not hang. Just verify it terminates.
	calls := 0
	EventuallyEvery(t, time.Second, 0, func() bool {
		calls++
		return calls >= 2
	})
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestEventuallyE_ImmediateSuccess(t *testing.T) {
	calls := 0
	err := EventuallyE(t, time.Second, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestEventuallyE_ReturnsLastErrorOnTimeout(t *testing.T) {
	sentinel := errors.New("still failing")
	err := EventuallyE(t, 15*time.Millisecond, func() error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}

func TestEventuallyE_ZeroTimeoutReturnsFirstError(t *testing.T) {
	sentinel := errors.New("boom")
	calls := 0
	err := EventuallyE(t, 0, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (single attempt)", calls)
	}
}

func TestEventuallyE_NilFnFatal(t *testing.T) {
	ExpectFatal(t, "TestEventuallyE_NilFnFatal", "XTEST_EVENTUALLY_E_NIL",
		"nil fn",
		func(t *testing.T) {
			_ = EventuallyE(t, time.Second, nil)
		})
}

func TestNever_StaysFalse(t *testing.T) {
	Never(t, 20*time.Millisecond, func() bool { return false })
}

func TestNever_FailsWhenTrue(t *testing.T) {
	ExpectFatal(t, "TestNever_FailsWhenTrue", "XTEST_NEVER_TRUE",
		"condition became true",
		func(t *testing.T) {
			Never(t, 20*time.Millisecond, func() bool { return true })
		})
}

func TestWaitForValue_Succeeds(t *testing.T) {
	got := 0
	v := WaitForValue(t, time.Second, 3, func() int {
		got++
		return got
	})
	if v != 3 {
		t.Fatalf("v = %d, want 3", v)
	}
}

func TestWaitForValue_FailsOnTimeout(t *testing.T) {
	ExpectFatal(t, "TestWaitForValue_FailsOnTimeout", "XTEST_WFV_TIMEOUT",
		"got 0, want 9",
		func(t *testing.T) {
			WaitForValue(t, 15*time.Millisecond, 9, func() int { return 0 })
		})
}

func TestWaitForValue_NilGetterFatal(t *testing.T) {
	ExpectFatal(t, "TestWaitForValue_NilGetterFatal", "XTEST_WFV_NIL",
		"nil get",
		func(t *testing.T) {
			WaitForValue[int](t, time.Second, 0, nil)
		})
}
