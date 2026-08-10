package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := runManager(tt.arguments)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runManager() error = %v, want %q", err, tt.want)
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
