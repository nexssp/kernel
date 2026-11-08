package action

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/xerr"
)

type renewalLossOnCancelMutex struct {
	started chan struct{}
}

func (m *renewalLossOnCancelMutex) Acquire(_ context.Context, key string, _ time.Duration) (LockLease, bool, error) {
	return LockLease{Key: key, Owner: "owner", Fence: 1}, true, nil
}

func (m *renewalLossOnCancelMutex) Renew(ctx context.Context, _ LockLease, _ time.Duration) (bool, error) {
	close(m.started)
	<-ctx.Done()
	return false, nil
}

func (*renewalLossOnCancelMutex) Release(context.Context, LockLease) (bool, error) {
	return true, nil
}

func TestExclusiveFencedReportsRenewalLossDuringShutdown(t *testing.T) {
	mutex := &renewalLossOnCancelMutex{started: make(chan struct{})}
	built := New("fenced.renewal-race", func(ctx context.Context, _ string) (string, error) {
		select {
		case <-mutex.started:
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}).ExclusiveFenced(mutex, 300*time.Millisecond, func(req string) string { return req }).Build()

	result := make(chan error, 1)
	go func() {
		_, err := built.Do(context.Background(), "key")
		result <- err
	}()
	select {
	case err := <-result:
		if xerr.KindFrom(err) != xerr.KindUnavailable {
			t.Fatalf("handler result error = %v, want lease-loss Unavailable", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fenced action did not finish after renewal loss")
	}
}
