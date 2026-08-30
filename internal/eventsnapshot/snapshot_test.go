package eventsnapshot

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeBoundsSnapshot(t *testing.T) {
	document, err := Decode([]byte(`{"repository":{"id":9007199254740993,"full_name":"acme/example"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if document["repository"].(map[string]any)["id"] != json.Number("9007199254740993") {
		t.Fatalf("decoded snapshot = %#v", document)
	}
	if _, err := Decode([]byte(`{"first":1}{"second":2}`)); err == nil {
		t.Fatal("Decode() accepted trailing JSON")
	}
	if _, err := Decode([]byte(strings.Repeat(" ", MaxBytes+1))); err == nil {
		t.Fatal("Decode() accepted an oversized snapshot")
	}
}
