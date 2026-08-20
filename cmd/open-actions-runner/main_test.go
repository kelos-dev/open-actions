package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/runner"
)

func TestRunWritesWorkflowJobResult(t *testing.T) {
	directory := t.TempDir()
	plan := runner.Plan{
		Version: runner.PlanVersion,
		Repository: runner.Repository{
			ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com",
		},
		Event:                 runner.Event{Name: "push", DeliveryID: "delivery"},
		Revision:              runner.Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName:          "CI",
		JobID:                 "build",
		TimeoutSeconds:        int64((6 * time.Hour) / time.Second),
		CleanupTimeoutSeconds: int64(runner.CleanupTimeout / time.Second),
		Outputs:               map[string]string{"value": "${{ steps.producer.outputs.value }}"},
		Steps: []runner.Step{{
			ID: "producer", Run: `test -z "$OPEN_ACTIONS_GITHUB_TOKEN"
test -z "$OPEN_ACTIONS_ACTION_TOKEN"
echo 'value=ready' >> "$GITHUB_OUTPUT"`,
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
	t.Setenv(runner.GitHubTokenEnvVar, "installation-token")
	t.Setenv(runner.ActionTokenEnvVar, "action-installation-token")
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
	if result.Conclusion != runner.ResultConclusionSuccess || result.Outputs["value"] != "ready" {
		t.Fatalf("result = %#v", result)
	}
}

func TestWithoutEnvironmentVariables(t *testing.T) {
	environment := withoutEnvironmentVariables([]string{
		"PATH=/usr/bin",
		"OPEN_ACTIONS_GITHUB_TOKEN=secret",
		"OPEN_ACTIONS_GITHUB_TOKEN_BACKUP=preserved",
		"OPEN_ACTIONS_ACTION_TOKEN=secret",
		"OPEN_ACTIONS_ACTION_TOKEN_BACKUP=preserved",
	}, runner.GitHubTokenEnvVar, runner.ActionTokenEnvVar)
	if slices.Contains(environment, "OPEN_ACTIONS_GITHUB_TOKEN=secret") {
		t.Fatal("filtered environment contains the GitHub token")
	}
	if slices.Contains(environment, "OPEN_ACTIONS_ACTION_TOKEN=secret") {
		t.Fatal("filtered environment contains the action token")
	}
	if !slices.Contains(environment, "PATH=/usr/bin") || !slices.Contains(environment, "OPEN_ACTIONS_GITHUB_TOKEN_BACKUP=preserved") || !slices.Contains(environment, "OPEN_ACTIONS_ACTION_TOKEN_BACKUP=preserved") {
		t.Fatalf("filtered environment = %#v", environment)
	}
}

func TestLoadValuesReadsProjectedFiles(t *testing.T) {
	directory := t.TempDir()
	dataDirectory := filepath.Join(directory, "..2026_08_12_14_00_00.000000000")
	if err := os.Mkdir(dataDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDirectory, "TOKEN"), []byte("secret-value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(dataDirectory), filepath.Join(directory, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "TOKEN"), filepath.Join(directory, "TOKEN")); err != nil {
		t.Fatal(err)
	}
	values, err := loadValues(directory)
	if err != nil {
		t.Fatal(err)
	}
	if values["TOKEN"] != "secret-value" || len(values) != 1 {
		t.Fatalf("values = %#v", values)
	}
}
