package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/artifact"
	"github.com/kelos-dev/open-actions/internal/joblog"
	"github.com/kelos-dev/open-actions/internal/runner"
)

func TestRunWritesWorkflowJobResult(t *testing.T) {
	testRunArchivesWorkflowJobResult(t, false, 0, false)
}

func TestRunArchivesLogsAfterTimeout(t *testing.T) {
	testRunArchivesWorkflowJobResult(t, true, 0, false)
}

func TestRunContinuesStepsAndCleanupAfterArchiveFailure(t *testing.T) {
	testRunArchivesWorkflowJobResult(t, false, http.StatusUnauthorized, false)
}

func TestRunArchivesMaskedExecutorDiagnostics(t *testing.T) {
	testRunArchivesWorkflowJobResult(t, false, 0, true)
}

func TestRunTimeoutRunsCleanupDuringArchiveOutage(t *testing.T) {
	started := time.Now()
	testRunArchivesWorkflowJobResult(t, true, http.StatusServiceUnavailable, false)
	if time.Since(started) > 10*time.Second {
		t.Fatal("timeout waited for the archive retry deadline")
	}
}

func TestRunArchivesSetupFailures(t *testing.T) {
	planData, err := json.Marshal(runner.Plan{
		Version: runner.PlanVersion,
		Run:     runner.Run{ID: 1, Number: 1, Attempt: 1, Actor: "octocat"},
		Repository: runner.Repository{
			ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com",
		},
		Event:                  runner.Event{Name: "push", DeliveryID: "delivery"},
		Revision:               runner.Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName:           "CI",
		WorkflowPath:           ".github/workflows/ci.yml",
		JobID:                  "build",
		GitHubTokenPermissions: map[string]string{"contents": "read"},
		TimeoutSeconds:         60,
		CleanupTimeoutSeconds:  10,
		Steps:                  []runner.Step{{Run: "true"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedToken := base64.StdEncoding.EncodeToString([]byte("x-access-token:installation-token"))
	for _, test := range []struct {
		name      string
		plan      string
		arguments []string
		secret    string
		want      string
	}{
		{name: "plan version", plan: `{"version":999}`, want: "unsupported job plan version"},
		{name: "masked plan error", plan: fmt.Sprintf(`{"%s":true}`, encodedToken), want: "unknown field"},
		{name: "executor configuration", plan: string(planData), want: "runner executor configuration is incomplete"},
		{name: "flag parsing", arguments: []string{"--unknown-option"}, want: "flag provided but not defined"},
		{name: "masked Project Secret", plan: string(planData), secret: "project-secret-value", want: "load Project variables"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var chunks []joblog.Chunk
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPut || request.URL.Path != joblog.Path || request.Header.Get("Authorization") != "Bearer artifact-token" {
					t.Errorf("unexpected upload: %s %s", request.Method, request.URL.Path)
				}
				var chunk joblog.Chunk
				if err := json.NewDecoder(request.Body).Decode(&chunk); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				mu.Lock()
				chunks = append(chunks, chunk)
				mu.Unlock()
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			t.Setenv(joblog.URLEnvVar, server.URL)
			t.Setenv(runner.ArtifactTokenEnvVar, "artifact-token")
			t.Setenv(runner.GitHubTokenEnvVar, "installation-token")
			t.Setenv(runner.ActionTokenEnvVar, "action-token")
			t.Setenv(runner.RunnerNameEnvVar, "")
			directory := t.TempDir()
			planPath := filepath.Join(directory, "plan.json")
			if err := os.WriteFile(planPath, []byte(test.plan), 0o600); err != nil {
				t.Fatal(err)
			}
			arguments := append([]string{"--job-file=" + planPath}, test.arguments...)
			if test.secret != "" {
				secrets := t.TempDir()
				if err := os.WriteFile(filepath.Join(secrets, "VALUE"), []byte(test.secret), 0o600); err != nil {
					t.Fatal(err)
				}
				arguments = append(arguments, "--secrets-directory="+secrets, "--variables-directory="+filepath.Join(directory, test.secret))
			}
			err := run(context.Background(), arguments)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("setup error = %v, want %q", err, test.want)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(chunks) == 0 || !chunks[len(chunks)-1].Complete {
				t.Fatalf("setup archive was not finalized: %#v", chunks)
			}
			var archived bytes.Buffer
			for index, chunk := range chunks {
				if chunk.Sequence != uint64(index) {
					t.Fatalf("sequence = %d, want %d", chunk.Sequence, index)
				}
				archived.Write(chunk.Data)
			}
			plain, readErr := io.ReadAll(joblog.WithoutTimestamps(bytes.NewReader(archived.Bytes())))
			if readErr != nil || !strings.Contains(string(plain), err.Error()) {
				t.Fatalf("setup archive = %q, read error = %v", plain, readErr)
			}
			for _, secret := range []string{"installation-token", "action-token", "artifact-token", encodedToken, "project-secret-value"} {
				if strings.Contains(archived.String(), secret) {
					t.Fatalf("setup archive exposed secret %q", secret)
				}
			}
		})
	}
}

func TestArchiveSetupErrorBoundsDiagnostic(t *testing.T) {
	chunks := make(chan joblog.Chunk, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var received joblog.Chunk
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		chunks <- received
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := archiveSetupError(ctx, server.URL, "token", strings.Repeat("x", joblog.MaxChunkBytes)); err != nil {
		t.Fatal(err)
	}
	received := <-chunks
	if len(received.Data) > joblog.MaxChunkBytes || !received.Complete || !bytes.HasSuffix(received.Data, []byte(" [diagnostic truncated]\n")) {
		t.Fatalf("setup diagnostic size = %d, complete = %v", len(received.Data), received.Complete)
	}
}

func testRunArchivesWorkflowJobResult(t *testing.T, timeout bool, archiveStatus int, invalidCondition bool) {
	t.Helper()
	directory := t.TempDir()
	logStore, err := joblog.NewStore(filepath.Join(directory, "logs"), time.Hour, 1<<20, joblog.DefaultMaxChunks)
	if err != nil {
		t.Fatal(err)
	}
	artifactStore, err := artifact.NewStore(filepath.Join(directory, "artifacts"), artifact.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := artifact.NewTokenCodec(bytes.Repeat([]byte("k"), 32))
	if err != nil {
		t.Fatal(err)
	}
	readTokens, err := joblog.NewReadTokenCodec(bytes.Repeat([]byte("r"), 32))
	if err != nil {
		t.Fatal(err)
	}
	service, err := artifact.NewServer(artifact.ServerConfig{Address: ":0", PublicURL: "http://artifacts.example", Store: artifactStore, Logs: logStore, LogReads: readTokens, Tokens: tokens, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if archiveStatus != 0 {
			writer.WriteHeader(archiveStatus)
			return
		}
		service.Handler().ServeHTTP(writer, request)
	}))
	defer server.Close()
	token, err := tokens.NewRuntimeToken(time.Now(), time.Hour, artifact.TokenClaims{Scope: artifact.Scope{ProjectUID: "project", RepositoryID: 1, RootRunUID: "run", RunUID: "run", Attempt: 1}, WorkflowRunBackendID: "run", WorkflowJobBackendID: "job"})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(joblog.URLEnvVar, server.URL)
	t.Setenv(runner.ArtifactTokenEnvVar, token)

	plan := runner.Plan{
		Version: runner.PlanVersion,
		Run:     runner.Run{ID: 1, Number: 1, Attempt: 1, Actor: "octocat"},
		Repository: runner.Repository{
			ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com",
		},
		Event:                  runner.Event{Name: "push", DeliveryID: "delivery"},
		Revision:               runner.Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName:           "CI",
		WorkflowPath:           ".github/workflows/ci.yml",
		JobID:                  "build",
		GitHubTokenPermissions: map[string]string{"contents": "read"},
		TimeoutSeconds:         int64((6 * time.Hour) / time.Second),
		CleanupTimeoutSeconds:  int64(runner.CleanupTimeout / time.Second),
		Outputs:                map[string]string{"value": "${{ steps.producer.outputs.value }}"},
		Steps: []runner.Step{{
			ID: "producer", Run: `test -z "$OPEN_ACTIONS_GITHUB_TOKEN"
test -z "$OPEN_ACTIONS_ACTION_TOKEN"
test "$RUNNER_NAME" = runner-1
test "$RUNNER_ENVIRONMENT" = self-hosted
test "${{ runner.name }}" = runner-1
test "${{ runner.environment }}" = self-hosted
echo '::add-mask::dynamic-secret'
echo 'installation-token action-installation-token dynamic-secret'
echo "$ACTIONS_RUNTIME_TOKEN"
echo 'value=ready' >> "$GITHUB_OUTPUT"`,
		}},
	}
	if timeout {
		plan.TimeoutSeconds = 1
		plan.CleanupTimeoutSeconds = 10
		if archiveStatus != 0 {
			plan.CleanupTimeoutSeconds = 1
		}
	}
	var containerOutput *os.File
	if archiveStatus != 0 {
		containerOutput, err = os.CreateTemp(directory, "container-output")
		if err != nil {
			t.Fatal(err)
		}
		defer containerOutput.Close()
		originalOutput := os.Stdout
		os.Stdout = containerOutput
		defer func() { os.Stdout = originalOutput }()
		// A shell builtin closes the output pipe when the timed-out shell is killed.
		plan.Steps = append(plan.Steps,
			runner.Step{Run: `printf '%05000000d\nlarge output done\n' 0`},
			runner.Step{If: "always()", Run: "echo cleanup-output; touch cleanup-ran"},
		)
	}
	if timeout {
		plan.Steps = append(plan.Steps, runner.Step{Name: "Wait for timeout", Run: "echo before-timeout; exec sleep 30"})
	}
	if invalidCondition {
		plan.Steps = append(plan.Steps, runner.Step{If: "not_a_context.value", Run: "true"})
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
	t.Setenv(runner.RunnerNameEnvVar, "runner-1")
	executionError := run(context.Background(), []string{"--job-file=" + planPath, "--result-file=" + resultPath, "--workspace=" + filepath.Join(directory, "workspace")})
	if (timeout || archiveStatus != 0 || invalidCondition) && executionError == nil {
		t.Fatal("runner succeeded after an execution or archive failure")
	}
	if !timeout && archiveStatus == 0 && !invalidCondition && executionError != nil {
		t.Fatal(executionError)
	}
	resultData, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.DecodeResult(resultData)
	if err != nil {
		t.Fatal(err)
	}
	wantConclusion := runner.ResultConclusionSuccess
	if archiveStatus != 0 || invalidCondition {
		wantConclusion = runner.ResultConclusionFailure
	}
	if timeout {
		wantConclusion = runner.ResultConclusionTimedOut
	}
	if result.Conclusion != wantConclusion || result.Outputs["value"] != "ready" {
		t.Fatalf("result = %#v", result)
	}

	if archiveStatus != 0 {
		if _, err := os.Stat(filepath.Join(directory, "workspace", "cleanup-ran")); err != nil {
			t.Fatalf("cleanup did not run: %v", err)
		}
		info, err := containerOutput.Stat()
		if err != nil {
			t.Fatal(err)
		}
		tail := make([]byte, min(info.Size(), int64(1024)))
		if _, err := containerOutput.ReadAt(tail, info.Size()-int64(len(tail))); err != nil {
			t.Fatal(err)
		}
		if !timeout && !strings.Contains(string(tail), "large output done\n") || !strings.Contains(string(tail), "cleanup-output\n") {
			t.Fatalf("container output stopped after archive failure: %s", tail)
		}
		return
	}
	var archived bytes.Buffer
	for sequence := uint64(0); ; sequence++ {
		chunk, err := logStore.Read("job", sequence)
		if err != nil {
			t.Fatal(err)
		}
		archived.Write(chunk.Data)
		if chunk.Complete {
			break
		}
	}
	plain, err := io.ReadAll(joblog.WithoutTimestamps(bytes.NewReader(archived.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if executionError != nil && !strings.Contains(string(plain), executionError.Error()) {
		t.Fatalf("missing executor diagnostic %q in archive: %s", executionError, plain)
	}
	for _, secret := range []string{"installation-token", "action-installation-token", "dynamic-secret", token} {
		if strings.Contains(archived.String(), secret) {
			t.Fatalf("archive exposed secret %q", secret)
		}
	}
	if !strings.Contains(archived.String(), "***") || !strings.Contains(archived.String(), "workflow step") {
		t.Fatalf("missing masked output or lifecycle records: %s", archived.String())
	}
	if timeout && !strings.Contains(archived.String(), "before-timeout\n") {
		t.Fatalf("missing output from the timed-out step: %s", archived.String())
	}
}

func TestRunLoadsNeedsContext(t *testing.T) {
	directory := t.TempDir()
	plan := runner.Plan{
		Version: runner.PlanVersion,
		Run:     runner.Run{ID: 1, Number: 1, Attempt: 1, Actor: "octocat"},
		Repository: runner.Repository{
			ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com",
		},
		Event:                  runner.Event{Name: "push", DeliveryID: "delivery"},
		Revision:               runner.Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName:           "CI",
		WorkflowPath:           ".github/workflows/ci.yml",
		JobID:                  "report",
		GitHubTokenPermissions: map[string]string{"contents": "read"},
		TimeoutSeconds:         int64((6 * time.Hour) / time.Second),
		CleanupTimeoutSeconds:  int64(runner.CleanupTimeout / time.Second),
		Outputs:                map[string]string{"artifact": "${{ needs.build.outputs.artifact }}"},
		Steps: []runner.Step{{
			If:  "needs.build.result == 'success'",
			Run: `test "${{ needs.build.outputs.artifact }}" = ready`,
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
	needsData, err := runner.EncodeNeedsContext(runner.Needs{"build": {Result: "success", Outputs: map[string]string{"artifact": "ready"}}})
	if err != nil {
		t.Fatal(err)
	}
	needsPath := filepath.Join(directory, "needs.json")
	if err := os.WriteFile(needsPath, needsData, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runner.GitHubTokenEnvVar, "installation-token")
	t.Setenv(runner.ActionTokenEnvVar, "action-installation-token")
	t.Setenv(runner.RunnerNameEnvVar, "runner-1")
	resultPath := filepath.Join(directory, "result.json")
	if err := run(context.Background(), []string{
		"--job-file=" + planPath,
		"--needs-file=" + needsPath,
		"--result-file=" + resultPath,
		"--workspace=" + filepath.Join(directory, "workspace"),
	}); err != nil {
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
	if result.Conclusion != runner.ResultConclusionSuccess || result.Outputs["artifact"] != "ready" {
		t.Fatalf("result = %#v", result)
	}
}

func TestWithoutEnvironmentVariables(t *testing.T) {
	environment := withoutEnvironmentVariables([]string{
		"PATH=/usr/bin",
		"ACTIONS_RUNTIME_TOKEN=artifact-token",
		"OPEN_ACTIONS_GITHUB_TOKEN=secret",
		"OPEN_ACTIONS_GITHUB_TOKEN_BACKUP=preserved",
		"OPEN_ACTIONS_ACTION_TOKEN=secret",
		"OPEN_ACTIONS_ACTION_TOKEN_BACKUP=preserved",
		"RUNNER_NAME=runner-1",
	}, runner.GitHubTokenEnvVar, runner.ActionTokenEnvVar, runner.RunnerNameEnvVar)
	if slices.Contains(environment, "OPEN_ACTIONS_GITHUB_TOKEN=secret") {
		t.Fatal("filtered environment contains the GitHub token")
	}
	if slices.Contains(environment, "OPEN_ACTIONS_ACTION_TOKEN=secret") {
		t.Fatal("filtered environment contains the action token")
	}
	if slices.Contains(environment, "RUNNER_NAME=runner-1") {
		t.Fatal("filtered environment contains the runner name")
	}
	if !slices.Contains(environment, "PATH=/usr/bin") || !slices.Contains(environment, "ACTIONS_RUNTIME_TOKEN=artifact-token") || !slices.Contains(environment, "OPEN_ACTIONS_GITHUB_TOKEN_BACKUP=preserved") || !slices.Contains(environment, "OPEN_ACTIONS_ACTION_TOKEN_BACKUP=preserved") {
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
