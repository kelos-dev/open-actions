package endpointurl

import "testing"

func TestNormalizeOrigin(t *testing.T) {
	got, err := NormalizeOrigin("https://actions.example/", "Console URL")
	if err != nil || got != "https://actions.example" {
		t.Fatalf("NormalizeOrigin() = %q, %v", got, err)
	}
	if _, err := NormalizeOrigin("https://actions.example/console", "Console URL"); err == nil {
		t.Fatal("NormalizeOrigin() accepted a path")
	}
}
