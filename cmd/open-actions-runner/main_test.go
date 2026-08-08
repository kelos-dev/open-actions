package main

import (
	"slices"
	"testing"
)

func TestWithoutEnvironmentVariable(t *testing.T) {
	environment := withoutEnvironmentVariable([]string{
		"PATH=/usr/bin",
		"OPEN_ACTIONS_GITHUB_TOKEN=secret",
		"OPEN_ACTIONS_GITHUB_TOKEN_BACKUP=preserved",
	}, "OPEN_ACTIONS_GITHUB_TOKEN")
	if slices.Contains(environment, "OPEN_ACTIONS_GITHUB_TOKEN=secret") {
		t.Fatal("filtered environment contains the GitHub token")
	}
	if !slices.Contains(environment, "PATH=/usr/bin") || !slices.Contains(environment, "OPEN_ACTIONS_GITHUB_TOKEN_BACKUP=preserved") {
		t.Fatalf("filtered environment = %#v", environment)
	}
}
