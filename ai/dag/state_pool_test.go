package dag

import "testing"

func TestStateReleaseIsIdempotent(t *testing.T) {
	s := AcquireState()
	s.Set("key", "value")
	s.Release()
	s.Release()

	next := AcquireState()
	next.Set("fresh", "value")
	if _, ok := next.Get("key"); ok {
		t.Fatal("released state retained data")
	}
	next.Release()
}
