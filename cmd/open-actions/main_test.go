package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunShowsHelp(t *testing.T) {
	for _, arguments := range [][]string{nil, {"help"}, {"--help"}} {
		var stdout bytes.Buffer
		if err := run(context.Background(), arguments, &stdout, &bytes.Buffer{}); err != nil {
			t.Fatalf("run(%v) error = %v", arguments, err)
		}
		if !strings.Contains(stdout.String(), "open-actions <command>") || !strings.Contains(stdout.String(), "install") {
			t.Fatalf("run(%v) output = %q", arguments, stdout.String())
		}
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	err := run(context.Background(), []string{"unknown"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), `unknown command "unknown"`) {
		t.Fatalf("run() error = %v, want unknown command", err)
	}
}

func TestRunInstallHelp(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(context.Background(), []string{"install", "--help"}, &bytes.Buffer{}, &stderr); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.Contains(stderr.String(), "Usage: open-actions install [--values FILE]") ||
		!strings.Contains(stderr.String(), "-values string") {
		t.Fatalf("run() stderr = %q", stderr.String())
	}
}

func TestRunInstallRejectsArguments(t *testing.T) {
	err := run(context.Background(), []string{"install", "extra"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || err.Error() != "open-actions install does not accept arguments" {
		t.Fatalf("run() error = %v", err)
	}
}
