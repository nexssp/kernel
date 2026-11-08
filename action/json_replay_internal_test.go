package action

import (
	"encoding/json"
	"testing"
)

type namedJSONResult any

func TestNormalizeJSONReplayResultForNamedEmptyInterface(t *testing.T) {
	var original namedJSONResult = int(7)
	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	got, err := normalizeJSONReplayResult(original, payload)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := got.(float64)
	if !ok || value != 7 {
		t.Fatalf("normalized result = (%#v, %t), want float64(7)", got, ok)
	}

	var replay namedJSONResult
	if err := json.Unmarshal(payload, &replay); err != nil {
		t.Fatal(err)
	}
	replayValue, ok := replay.(float64)
	if !ok || replayValue != value {
		t.Fatalf("replayed result = (%#v, %t), want the normalized float64(%v)", replay, ok, value)
	}
}
