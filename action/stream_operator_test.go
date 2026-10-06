package action

import (
	"strings"
	"testing"
)

func TestNormalizeConfigForTarget_BoolField(t *testing.T) {
	t.Parallel()

	type cfg struct {
		Quiet bool `json:"quiet"`
	}
	got, err := NormalizeConfigForTarget(map[string]any{"quiet": "true"}, cfg{})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("got = %T, want map[string]any", got)
	}
	if m["quiet"] != true {
		t.Fatalf("quiet = %v, want true", m["quiet"])
	}
}

func TestNormalizeConfigForTarget_StringFieldStaysString(t *testing.T) {
	t.Parallel()

	type cfg struct {
		Clipboard string `json:"clipboard"`
	}
	got, err := NormalizeConfigForTarget(map[string]any{"clipboard": "false"}, cfg{})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("got = %T, want map[string]any", got)
	}
	if m["clipboard"] != "false" {
		t.Fatalf("clipboard = %v, want \"false\"", m["clipboard"])
	}
}

func TestNormalizeConfigForTarget_InvalidBoolReportsField(t *testing.T) {
	t.Parallel()

	type cfg struct {
		Quiet bool `json:"quiet"`
	}
	_, err := NormalizeConfigForTarget(map[string]any{"quiet": "maybe"}, cfg{})
	if err == nil {
		t.Fatal("expected error for invalid bool")
	}
	if !strings.Contains(err.Error(), `field "quiet"`) {
		t.Fatalf("error = %v, want field diagnostic", err)
	}
}
