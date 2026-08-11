package runner

import (
	"maps"
	"os"
	"strings"
	"testing"
)

func TestRunnerResultRoundTrip(t *testing.T) {
	want, err := NewResult(map[string]string{"multiline": "first\nsecond", "value": "ready"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeResult(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeResult(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != ResultVersion || !maps.Equal(got.Outputs, want.Outputs) {
		t.Fatalf("decoded result = %#v, want %#v", got, want)
	}
}

func TestCommandFilesRejectOversizedStepOutputs(t *testing.T) {
	files, err := newCommandFiles(t.TempDir(), "step")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("value=" + strings.Repeat("x", maxOutputCommandFileBytes))
	if err := os.WriteFile(files.output, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := files.read(); err == nil || !strings.Contains(err.Error(), "GITHUB_OUTPUT") || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized output error = %v", err)
	}
}

func TestRunnerResultEnforcesBounds(t *testing.T) {
	tooMany := make(map[string]string, MaxJobOutputs+1)
	for index := 0; index <= MaxJobOutputs; index++ {
		tooMany["output_"+strings.Repeat("x", index)] = "value"
	}
	if _, err := NewResult(tooMany); err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("too many outputs error = %v", err)
	}
	if _, err := NewResult(map[string]string{"value": strings.Repeat("x", MaxResultBytes)}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized result error = %v", err)
	}
}

func TestDecodeResultRejectsInvalidDocuments(t *testing.T) {
	for _, data := range []string{
		`{"version":2}`,
		`{"version":1,"unknown":true}`,
		`{"version":1}{"version":1}`,
		`{"version":1,"outputs":{"invalid.name":"value"}}`,
	} {
		if _, err := DecodeResult([]byte(data)); err == nil {
			t.Fatalf("DecodeResult() accepted %s", data)
		}
	}
}
