package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunConsoleRejectsInvalidWorkflowRunTTL(t *testing.T) {
	for _, value := range []string{"-1", "2147483648", "invalid"} {
		err := runConsole(context.Background(), []string{"--workflow-run-ttl-seconds-after-finished=" + value})
		if err == nil || !strings.Contains(err.Error(), "must be an integer between") {
			t.Fatalf("runConsole() error = %v for TTL %q", err, value)
		}
	}
}

func TestReadTokenRejectsInvalidConfiguration(t *testing.T) {
	emptyPath := filepath.Join(t.TempDir(), "empty-token")
	if err := os.WriteFile(emptyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		path string
		want string
	}{
		{name: "missing path", want: "Console token file is required"},
		{name: "missing file", path: filepath.Join(t.TempDir(), "missing-token"), want: "read Console token"},
		{name: "empty token", path: emptyPath, want: "Console token is empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := readToken(test.path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("readToken() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestReadTokenTrimsWhitespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("  token-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := readToken(path)
	if err != nil || token != "token-value" {
		t.Fatalf("readToken() = %q, %v", token, err)
	}
}

func TestHealthy(t *testing.T) {
	response := httptest.NewRecorder()
	healthy(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK || response.Body.String() != "ok\n" {
		t.Fatalf("health response = %d, %q", response.Code, response.Body.String())
	}
}
