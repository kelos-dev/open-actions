package eventsnapshot

import (
	"strings"
	"testing"
)

func TestDecodeBoundsSnapshot(t *testing.T) {
	if _, err := Decode([]byte(`{"repository":{"full_name":"acme/example"}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := Decode([]byte(`{"first":1}{"second":2}`)); err == nil {
		t.Fatal("Decode() accepted trailing JSON")
	}
	if _, err := Decode([]byte(strings.Repeat(" ", MaxBytes+1))); err == nil {
		t.Fatal("Decode() accepted an oversized snapshot")
	}
}
