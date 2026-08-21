package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunManagerRejectsInvalidEndpointURLs(t *testing.T) {
	for _, tt := range []struct {
		name      string
		arguments []string
		want      string
	}{
		{name: "GitHub API URL", arguments: []string{"--github-api-url=https://user:password@github.example"}, want: "GitHub API URL must not include user information"},
		{name: "GitHub server URL", arguments: []string{"--github-server-url=git://github.example"}, want: "GitHub server URL must use http or https"},
		{name: "action clone base URL", arguments: []string{"--action-clone-base-url=https://github.example?token=secret"}, want: "action clone base URL must not include a query"},
		{name: "Console URL scheme", arguments: []string{"--console-url=git://actions.example"}, want: "Console URL must use http or https"},
		{name: "Console URL", arguments: []string{"--console-url=https://actions.example/open-actions"}, want: "Console URL must not include a path"},
		{name: "negative WorkflowRun TTL", arguments: []string{"--workflow-run-ttl-seconds-after-finished=-1"}, want: "must be an integer between"},
		{name: "excessive WorkflowRun TTL", arguments: []string{"--workflow-run-ttl-seconds-after-finished=2147483648"}, want: "must be an integer between"},
		{name: "short maximum job timeout", arguments: []string{"--max-job-timeout=59s"}, want: "max job timeout must be a positive whole number of minutes"},
		{name: "fractional maximum job timeout", arguments: []string{"--max-job-timeout=90s"}, want: "max job timeout must be a positive whole number of minutes"},
		{name: "artifact key without URL", arguments: []string{"--artifact-signing-key-file=/tmp/key"}, want: "must be specified together"},
		{name: "artifact URL without key", arguments: []string{"--artifact-service-url=https://artifacts.example"}, want: "must be specified together"},
		{name: "artifact URL scheme", arguments: []string{"--artifact-service-url=git://artifacts.example", "--artifact-signing-key-file=/tmp/key"}, want: "Artifact service URL must use http or https"},
		{name: "artifact retention", arguments: []string{"--artifact-service-url=https://artifacts.example", "--artifact-signing-key-file=/tmp/key", "--artifact-max-retention-days=0"}, want: "artifact maximum retention"},
		{name: "artifact key file", arguments: []string{"--artifact-service-url=https://artifacts.example", "--artifact-signing-key-file=/path/that/does/not/exist"}, want: "read artifact signing key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := runManager(tt.arguments)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runManager() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunManagerRejectsShortArtifactSigningKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signing-key")
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runManager([]string{"--artifact-service-url=https://artifacts.example", "--artifact-signing-key-file=" + path})
	if err == nil || !strings.Contains(err.Error(), "at least 32 bytes") {
		t.Fatalf("runManager() error = %v", err)
	}
}

func TestNormalizeActionCloneBaseURL(t *testing.T) {
	for _, tt := range []struct {
		name            string
		value           string
		githubServerURL string
		want            string
	}{
		{name: "GitHub server URL", githubServerURL: "https://github.example", want: "https://github.example"},
		{name: "explicit URL", value: "https://actions.example/git/", githubServerURL: "https://github.example", want: "https://actions.example/git"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeActionCloneBaseURL(tt.value, tt.githubServerURL)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("normalizeActionCloneBaseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWebhookServerRunsWithoutLeaderElectionAndTracksReadiness(t *testing.T) {
	runnable := &webhookServer{
		server: &http.Server{Addr: "127.0.0.1:0", Handler: http.NotFoundHandler()},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if runnable.NeedLeaderElection() {
		t.Fatal("HTTP server requires leader election")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runnable.Start(ctx) }()
	deadline := time.Now().Add(time.Second)
	for runnable.Readiness(nil) != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := runnable.Readiness(nil); err != nil {
		cancel()
		t.Fatalf("HTTP server did not become ready: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not stop")
	}
	if err := runnable.Readiness(nil); err == nil {
		t.Fatal("stopped HTTP server remained ready")
	}
}
