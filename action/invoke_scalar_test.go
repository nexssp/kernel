package action_test

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
)

func BenchmarkInvokeAnyStringScalarBinding(b *testing.B) {
	type request struct {
		Limit int `json:"limit"`
	}

	act := action.New("query.limit", func(_ context.Context, req request) (int, error) {
		return req.Limit, nil
	}).Build()
	input := map[string]any{"limit": "50"}
	ctx := context.Background()
	var result any

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		result, err = action.InvokeAny(ctx, act, input)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = result
}

func TestAssignStringScalars(t *testing.T) {
	type limit int8
	type identifier uint16
	type ratio float32
	type query struct {
		Enabled bool       `json:"enabled"`
		Limit   limit      `json:"limit"`
		ID      identifier `json:"id"`
		Ratio   ratio      `json:"ratio"`
	}

	var got query
	err := action.Assign(&got, map[string]any{
		"enabled": " true ",
		"limit":   " -12 ",
		"id":      " 65535 ",
		"ratio":   " 3.25 ",
	})
	if err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	want := query{Enabled: true, Limit: -12, ID: 65535, Ratio: 3.25}
	if got != want {
		t.Fatalf("Assign() = %#v, want %#v", got, want)
	}
}

func TestAssignStringScalarRejectsOutOfRange(t *testing.T) {
	type smallInt int8
	var got smallInt
	if err := action.Assign(&got, "128"); err == nil {
		t.Fatal("Assign() accepted an out-of-range int8 string")
	}
}
