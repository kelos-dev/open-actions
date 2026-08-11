package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/kelos-dev/open-actions/internal/runner"
)

func TestRunWritesWorkflowJobResult(t *testing.T) {
	directory := t.TempDir()
	plan := runner.Plan{
		Version: runner.PlanVersion,
		Repository: runner.Repository{
			ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com",
		},
		Event:        runner.Event{Name: "push", DeliveryID: "delivery"},
		Revision:     runner.Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName: "CI",
		JobID:        "build",
		Outputs:      map[string]string{"value": "${{ steps.producer.outputs.value }}"},
		Steps: []runner.Step{{
			ID: "producer", Run: `echo 'value=ready' >> "$GITHUB_OUTPUT"`,
		}},
	}
	planData, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(directory, "plan.json")
	if err := os.WriteFile(planPath, planData, 0o600); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(directory, "result.json")
	t.Setenv("OPEN_ACTIONS_GITHUB_TOKEN", "installation-token")
	if err := run(context.Background(), []string{"--job-file=" + planPath, "--result-file=" + resultPath, "--workspace=" + filepath.Join(directory, "workspace")}); err != nil {
		t.Fatal(err)
	}
	resultData, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.DecodeResult(resultData)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["value"] != "ready" {
		t.Fatalf("outputs = %#v", result.Outputs)
	}
}

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
