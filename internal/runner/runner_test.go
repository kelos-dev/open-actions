package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/actionref"
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	"github.com/kelos-dev/open-actions/internal/workflow"
)

func TestExecuteRunSteps(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Env = map[string]string{"JOB_ENV": "job", "SHARED": "job"}
	binDirectory := filepath.Join(workspace, "bin")
	if err := os.Mkdir(binDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	plan.Steps = []Step{
		{
			Name: "export values",
			Run:  `printf 'EXPORTED=value\n' >> "$GITHUB_ENV"; printf '%s\n' "$GITHUB_WORKSPACE/bin" >> "$GITHUB_PATH"; printf 'output=value\n' >> "$GITHUB_OUTPUT"; printf 'state=value\n' >> "$GITHUB_STATE"; printf 'summary\n' >> "$GITHUB_STEP_SUMMARY"`,
		},
		{
			Name: "write result",
			Run:  `printf '%s/%s/%s/%s/%s/%s/%s/%s' "$JOB_ENV" "$STEP_ENV" "$SHARED" "$JOB_ENV_CONTEXT" "$SHARED_CONTEXT" "$GITHUB_REF_NAME" "$EXPORTED" "${PATH%%:*}" > result`,
			Env:  map[string]string{"STEP_ENV": "step", "SHARED": "step", "JOB_ENV_CONTEXT": "${{ env.JOB_ENV }}", "SHARED_CONTEXT": "${{ env.SHARED }}"},
		},
	}
	executor := testExecutor(t, io.Discard, io.Discard)
	if err := executor.Execute(context.Background(), plan, workspace); err != nil {
		t.Fatalf("execute: %v", err)
	}
	result, err := os.ReadFile(filepath.Join(workspace, "result"))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "job/step/step/job/job/main/value/"+binDirectory {
		t.Errorf("result = %q", result)
	}
}

func TestExecuteResolvesHashFiles(t *testing.T) {
	workspace := t.TempDir()
	contents := []byte("locked dependency")
	if err := os.WriteFile(filepath.Join(workspace, "dependency.lock"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	plan.Steps = []Step{{Run: `printf '%s' "${{ hashFiles('**/*.lock') }}" > result`}}
	if err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, workspace); err != nil {
		t.Fatal(err)
	}
	result, err := os.ReadFile(filepath.Join(workspace, "result"))
	if err != nil {
		t.Fatal(err)
	}
	fileDigest := sha256.Sum256(contents)
	want := sha256.Sum256(fileDigest[:])
	if string(result) != fmt.Sprintf("%x", want) {
		t.Fatalf("result = %q, want %x", result, want)
	}
}

func TestExecuteExposesRunStepOutputs(t *testing.T) {
	plan := testPlan()
	plan.Outputs = map[string]string{
		"missing":     "${{ steps.producer.outputs.missing }}",
		"multiline":   "${{ steps.producer.outputs.multiline }}",
		"overwritten": "${{ steps.producer.outputs.value }}",
		"skipped":     "${{ steps.skipped.outputs.value }}",
	}
	plan.Steps = []Step{
		{
			ID:  "producer",
			Run: `printf '%s\n' 'value=first' 'value=second' 'multiline<<EOF' 'first line' 'second line' 'EOF' >> "$GITHUB_OUTPUT"`,
		},
		{ID: "skipped", If: "false", Run: `echo 'value=unavailable' >> "$GITHUB_OUTPUT"`},
		{
			Env: map[string]string{
				"MISSING":     "${{ steps.producer.outputs.missing }}",
				"MULTILINE":   "${{ steps.producer.outputs.multiline }}",
				"OVERWRITTEN": "${{ steps.producer.outputs.value }}",
				"SKIPPED":     "${{ steps.skipped.outputs.value }}",
			},
			Run: `test -z "$MISSING" && test "$MULTILINE" = $'first line\nsecond line' && test "$OVERWRITTEN" = second && test -z "$SKIPPED"`,
		},
	}
	result, err := testExecutor(t, io.Discard, io.Discard).ExecuteResult(context.Background(), plan, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"missing": "", "multiline": "first line\nsecond line", "overwritten": "second", "skipped": ""}
	if !maps.Equal(result.Outputs, want) {
		t.Fatalf("outputs = %#v, want %#v", result.Outputs, want)
	}
}

func TestExecuteKeepsOutputsFromFailedSteps(t *testing.T) {
	plan := testPlan()
	plan.Outputs = map[string]string{"value": "${{ steps.failed.outputs.value }}"}
	plan.Steps = []Step{
		{ID: "failed", Run: `echo 'value=available' >> "$GITHUB_OUTPUT"; exit 1`},
		{If: "always()", Run: `test '${{ steps.failed.outputs.value }}' = available`},
	}
	result, err := testExecutor(t, io.Discard, io.Discard).ExecuteResult(context.Background(), plan, t.TempDir())
	if err == nil {
		t.Fatal("ExecuteResult() succeeded after a failed step")
	}
	if result.Outputs["value"] != "available" {
		t.Fatalf("outputs = %#v", result.Outputs)
	}
}

func TestExecuteKeepsOutputsFromFailedCompositeActions(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "failed-output", "v1", map[string]string{
		"action.yml": `name: Failed output fixture
outputs:
  result:
    value: ${{ steps.failed.outputs.value }}
runs:
  using: composite
  steps:
    - id: failed
      run: echo 'value=available' >> "$GITHUB_OUTPUT"; exit 1
      shell: bash
`,
	})
	plan := testPlan()
	plan.Repository.ActionCloneBaseURL = "file://" + repositories
	plan.Outputs = map[string]string{"value": "${{ steps.failed.outputs.result }}"}
	plan.Steps = []Step{
		{ID: "failed", Uses: "actions/failed-output@v1"},
		{If: "always()", Run: `test '${{ steps.failed.outputs.result }}' = available`},
	}
	result, err := testExecutor(t, io.Discard, io.Discard).ExecuteResult(context.Background(), plan, t.TempDir())
	if err == nil {
		t.Fatal("ExecuteResult() succeeded after a failed composite action")
	}
	if result.Outputs["value"] != "available" {
		t.Fatalf("outputs = %#v", result.Outputs)
	}
}

func TestExecuteOmitsSecretDerivedJobOutputs(t *testing.T) {
	plan := testPlan()
	plan.Outputs = map[string]string{"secret": "prefix-${{ steps.secret.outputs.value }}"}
	plan.Steps = []Step{{
		ID:  "secret",
		Env: map[string]string{"TOKEN": "${{ github.token }}"},
		Run: `printf 'value=%s\n' "$TOKEN" >> "$GITHUB_OUTPUT"`,
	}}
	var logs bytes.Buffer
	result, err := testExecutor(t, &logs, &logs).ExecuteResult(context.Background(), plan, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := result.Outputs["secret"]; found {
		t.Fatalf("secret output was persisted: %#v", result.Outputs)
	}
	if strings.Contains(logs.String(), "installation-token") {
		t.Fatalf("secret output was logged: %s", logs.String())
	}
}

func TestExecuteResolvesProblemMatchersFromWorkspace(t *testing.T) {
	workspace := t.TempDir()
	matcherDirectory := filepath.Join(workspace, ".github")
	if err := os.MkdirAll(matcherDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	matcher := `{"problemMatcher":[{"owner":"test","pattern":{"regexp":"^(.+):(\\d+): (.*)$","file":1,"line":2,"message":3}}]}`
	if err := os.WriteFile(filepath.Join(matcherDirectory, "matcher.json"), []byte(matcher), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	plan.Steps = []Step{{
		Run:              `printf '%s\n' '::add-matcher::.github/matcher.json' 'source.go:7: broken'`,
		WorkingDirectory: "subdir",
	}}
	var output bytes.Buffer
	executor := testExecutor(t, &output, &output)
	if err := executor.Execute(context.Background(), plan, workspace); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(output.String(), "::error file=source.go,line=7::broken") {
		t.Fatalf("output has no matcher annotation: %q", output.String())
	}
}

func TestExecuteEvaluatesWorkflowExpressions(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Matrix = map[string]any{"arch": "arm64"}
	plan.Env = map[string]string{"ARCH": "${{ matrix.arch }}", "BRANCH": "${{ github.ref_name }}", "JOB_TOKEN": "${{ github.token }}"}
	plan.Outputs = map[string]string{"image": "${{ matrix.arch }}-${{ steps.build.outputs.image }}"}
	plan.Steps = []Step{
		{
			Name: "skipped",
			If:   "github.ref_name != 'main'",
			Run:  "touch skipped ${{ secrets.TOKEN }}",
			Env:  map[string]string{"TOKEN": "${{ secrets.TOKEN }}"},
		},
		{
			ID:   "build",
			Name: "write ${{ github.ref_name }} result",
			If:   "env.TARGET == 'main'",
			Env: map[string]string{
				"REPOSITORY": "${{ github.repository }}",
				"STEP_TOKEN": "${{ github.token }}",
				"TARGET":     "main",
			},
			Run: "test \"$JOB_TOKEN\" = installation-token && test \"$STEP_TOKEN\" = installation-token && printf '%s/%s/${{ github.sha }}/${{ env.TARGET }}/${{ matrix.arch }}' \"$BRANCH\" \"$REPOSITORY\" > result && echo 'image=ready' >> \"$GITHUB_OUTPUT\"",
		},
	}
	executor := testExecutor(t, io.Discard, io.Discard)
	executionResult, err := executor.ExecuteResult(context.Background(), plan, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if executionResult.Outputs["image"] != "arm64-ready" {
		t.Fatalf("job outputs = %#v", executionResult.Outputs)
	}
	if _, err := os.Stat(filepath.Join(workspace, "skipped")); !os.IsNotExist(err) {
		t.Fatalf("skipped step created a file: %v", err)
	}
	result, err := os.ReadFile(filepath.Join(workspace, "result"))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "main/acme/example/"+strings.Repeat("a", 40)+"/main/arm64" {
		t.Fatalf("result = %q", result)
	}
}

func TestExecuteResolvesAndMasksRepositoryValues(t *testing.T) {
	secret := "repository-secret-value"
	encodedSecret := base64.StdEncoding.EncodeToString([]byte(secret))
	plan := testPlan()
	plan.Env = map[string]string{
		"BUILTIN_TOKEN":    "${{ secrets.GITHUB_TOKEN }}",
		"NAMESPACE":        "${{ vars.deployment_namespace }}",
		"REPOSITORY_TOKEN": "${{ secrets.repository_token }}",
	}
	plan.Outputs = map[string]string{"secret": "${{ steps.value.outputs.secret }}"}
	plan.Steps = []Step{{
		ID: "value",
		Run: fmt.Sprintf(
			`test "$BUILTIN_TOKEN" = installation-token && test "$REPOSITORY_TOKEN" = %q && test "$NAMESPACE" = production && printf 'secret=%%s\n' "$REPOSITORY_TOKEN" >> "$GITHUB_OUTPUT" && printf '%%s\n%%s\n%%s\naction-installation-token\n' "$REPOSITORY_TOKEN" %q "$NAMESPACE"`,
			secret,
			encodedSecret,
		),
	}}
	var output bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewTextHandler(&output, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Secrets:     map[string]string{"REPOSITORY_TOKEN": secret},
		Variables:   map[string]string{"DEPLOYMENT_NAMESPACE": "production"},
		Environment: os.Environ(),
		Stdout:      &output,
		Stderr:      &output,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ExecuteResult(context.Background(), plan, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, found := result.Outputs["secret"]; found {
		t.Fatalf("secret-derived output was persisted: %#v", result.Outputs)
	}
	if strings.Contains(output.String(), secret) || strings.Contains(output.String(), encodedSecret) || strings.Contains(output.String(), "action-installation-token") {
		t.Fatalf("runner output exposed a credential: %s", output.String())
	}
	if !strings.Contains(output.String(), "production") || !strings.Contains(output.String(), "***") {
		t.Fatalf("runner output = %q", output.String())
	}
}

func TestOutputMaskerMasksSecretEncodingsAndLines(t *testing.T) {
	secret := `"alpha' beta&gamma<delta>"`
	masker := newOutputMasker()
	masker.addSecret(secret)
	jsonValue, err := json.Marshal(secret)
	if err != nil {
		t.Fatal(err)
	}
	forms := []string{
		secret,
		base64.StdEncoding.EncodeToString([]byte(secret)),
		base64.RawStdEncoding.EncodeToString([]byte(secret[1:])),
		string(jsonValue[1 : len(jsonValue)-1]),
		strings.ReplaceAll(url.QueryEscape(secret), "+", "%20"),
		strings.ReplaceAll(secret, `"`, `\"`),
		strings.ReplaceAll(secret, `'`, `''`),
		"&quot;alpha&apos; beta&amp;gamma&lt;delta&gt;&quot;",
		secret[1 : len(secret)-1],
	}
	for _, form := range forms {
		if masked := masker.mask(form); masked != "***" {
			t.Errorf("masked form = %q, want redacted", masked)
		}
	}

	masker = newOutputMasker()
	masker.addSecret("first secret line\n  second secret line  ")
	for _, line := range []string{"first secret line", "second secret line"} {
		if masked := masker.mask(line); masked != "***" {
			t.Errorf("masked line = %q, want redacted", masked)
		}
	}

	masker = newOutputMasker()
	masker.addSecret("{\n  \"private_key\": \"long-secret-value\"\n}")
	if masked := masker.mask("New version {\"result\":\"ready\"}"); masked != "New version {\"result\":\"ready\"}" {
		t.Errorf("short multiline fragments corrupted output: %q", masked)
	}
	if masker.contains(`{"result":"ready"}`) {
		t.Fatal("short multiline fragments marked a non-secret output as sensitive")
	}
	if masked := masker.mask("encoded fragments ew fQ"); masked != "encoded fragments ew fQ" {
		t.Errorf("encoded short multiline fragments corrupted output: %q", masked)
	}
	if masked := masker.mask(`"private_key": "long-secret-value"`); masked != "***" {
		t.Errorf("long multiline fragment was not masked: %q", masked)
	}

	masker = newOutputMasker()
	masker.addSecret("secretpart1&+secretpart2&secretpart3")
	for _, section := range []string{"secretpart1&+", "ecretpart2&secretpart3"} {
		if masked := masker.mask(section); masked != "***" {
			t.Errorf("masked PowerShell section = %q, want redacted", masked)
		}
	}
}

func TestExecuteRunsFailureAndAlwaysSteps(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Steps = []Step{
		{Run: "exit 1"},
		{Run: "touch default"},
		{If: "failure()", Run: "touch failure"},
		{If: "always()", Run: "touch always"},
	}
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, workspace)
	if err == nil {
		t.Fatal("Execute() succeeded after a failed step")
	}
	if _, err := os.Stat(filepath.Join(workspace, "default")); !os.IsNotExist(err) {
		t.Fatalf("default step ran after a failure: %v", err)
	}
	for _, name := range []string{"failure", "always"} {
		if _, err := os.Stat(filepath.Join(workspace, name)); err != nil {
			t.Fatalf("%s step did not run: %v", name, err)
		}
	}
}

func TestExecuteRunsCancelledStepsWithCleanupContext(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Steps = []Step{
		{Run: "touch default"},
		{If: "cancelled()", Run: "touch cancelled"},
		{If: "always()", Run: "touch always"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := testExecutor(t, io.Discard, io.Discard).Execute(ctx, plan, workspace); err == nil {
		t.Fatal("Execute() succeeded after cancellation")
	}
	if _, err := os.Stat(filepath.Join(workspace, "default")); !os.IsNotExist(err) {
		t.Fatalf("default step ran after cancellation: %v", err)
	}
	for _, name := range []string{"cancelled", "always"} {
		if _, err := os.Stat(filepath.Join(workspace, name)); err != nil {
			t.Fatalf("%s cleanup step did not run: %v", name, err)
		}
	}
}

func TestExecuteKeepsCancellationDistinctFromFailure(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "cleanup", "v1", map[string]string{
		"action.yml": `name: Cleanup fixture
runs:
  using: composite
  steps:
    - run: touch composite-started; exec sleep 30
      shell: bash
    - if: failure()
      run: touch composite-failure
      shell: bash
    - if: true
      run: touch composite-plain-condition
      shell: bash
    - if: cancelled()
      run: touch composite-cancelled
      shell: bash
`,
	})
	for _, cancelBeforeAction := range []bool{true, false} {
		name := "during action"
		if cancelBeforeAction {
			name = "before action"
		}
		t.Run(name, func(t *testing.T) {
			workspace := t.TempDir()
			plan := testPlan()
			plan.Repository.ActionCloneBaseURL = "file://" + repositories
			plan.Steps = []Step{
				{If: "always()", Uses: "actions/cleanup@v1"},
				{If: "failure()", Run: "touch workflow-failure"},
				{If: "cancelled()", Run: "touch workflow-cancelled"},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelBeforeAction {
				cancel()
			}
			result := make(chan error, 1)
			executor := testExecutor(t, io.Discard, io.Discard)
			go func() {
				result <- executor.Execute(ctx, plan, workspace)
			}()
			if !cancelBeforeAction {
				waitForFile(t, filepath.Join(workspace, "composite-started"), "composite step did not start")
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("Execute() succeeded after cancellation")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("canceled workflow did not stop")
			}
			for _, path := range []string{"composite-cancelled", "workflow-cancelled"} {
				if _, err := os.Stat(filepath.Join(workspace, path)); err != nil {
					t.Fatalf("%s did not run: %v", path, err)
				}
			}
			for _, path := range []string{"composite-failure", "composite-plain-condition", "workflow-failure"} {
				if _, err := os.Stat(filepath.Join(workspace, path)); !os.IsNotExist(err) {
					t.Fatalf("%s ran after cancellation: %v", path, err)
				}
			}
		})
	}
}

func TestExecuteRejectsOversizedEvaluatedContent(t *testing.T) {
	plan := testPlan()
	plan.Revision.HeadRef = strings.Repeat("x", workflow.MaxRunScriptBytes+1)
	plan.Steps = []Step{{Run: "${{ github.head_ref }}"}}
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "evaluated run script exceeds") {
		t.Fatalf("error = %v, want evaluated run script limit", err)
	}

	plan = testPlan()
	plan.Revision.HeadRef = strings.Repeat("x", 60_000)
	plan.Steps = []Step{
		{Run: "true", Env: map[string]string{"VALUE": "${{ github.head_ref }}"}},
		{Run: "true", Env: map[string]string{"VALUE": "${{ github.head_ref }}"}},
	}
	err = testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "evaluated job configuration exceeds") {
		t.Fatalf("error = %v, want evaluated job configuration limit", err)
	}
}

func TestResolvedStepBytesEnforcesFieldLimits(t *testing.T) {
	tests := map[string]Step{
		"name":              {Name: strings.Repeat("x", workflow.MaxStepNameLength+1)},
		"action reference":  {Uses: strings.Repeat("x", workflow.MaxActionReferenceLength+1)},
		"run script":        {Run: strings.Repeat("x", workflow.MaxRunScriptBytes+1)},
		"working directory": {WorkingDirectory: strings.Repeat("x", workflow.MaxWorkingDirectoryLength+1)},
		"with value":        {With: map[string]string{"value": strings.Repeat("x", workflow.MaxMapValueBytes+1)}},
		"environment value": {Env: map[string]string{"VALUE": strings.Repeat("x", workflow.MaxMapValueBytes+1)}},
	}
	for name, step := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := resolvedStepBytes(step); err == nil {
				t.Fatal("resolvedStepBytes() accepted an oversized field")
			}
		})
	}
}

func TestExecuteUsesEmptyStringForMissingRepositorySecret(t *testing.T) {
	plan := testPlan()
	plan.Env = map[string]string{"TOKEN": "${{ secrets.TOKEN }}"}
	plan.Steps = []Step{{Run: `test -z "$TOKEN"`}}
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadPlanSupportsCompatibleVersions(t *testing.T) {
	for version := minimumPlanVersion; version <= PlanVersion; version++ {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "job.json")
			data := fmt.Sprintf(`{"version":%d,"repository":{"id":1,"owner":"acme","name":"example","serverURL":"https://github.com","apiURL":"https://api.github.com","actionCloneBaseURL":"https://github.com"},"event":{"name":"push","deliveryID":"delivery"},"revision":{"sha":"abc","ref":"refs/heads/main","refName":"main"},"workflowName":"CI","jobID":"build","steps":[{"run":"true"}]}`, version)
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			plan, err := LoadPlan(path)
			if err != nil {
				t.Fatalf("load plan: %v", err)
			}
			if plan.Version != version || plan.JobID != "build" {
				t.Errorf("plan = %#v", plan)
			}
		})
	}
}

func TestLoadPlanValidatesVersionFivePullRequestRevision(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tt := range []struct {
		name     string
		action   string
		revision Revision
		wantErr  bool
	}{
		{name: "remote revision", revision: Revision{SHA: sha, Ref: "refs/pull/42/merge", RefName: "42/merge"}},
		{name: "head revision", revision: Revision{SHA: sha, HeadSHA: strings.Repeat("b", 40), Ref: "refs/pull/42/merge", RefName: "42/merge"}},
		{name: "integration revision", revision: Revision{SHA: sha, HeadSHA: strings.Repeat("b", 40), BaseSHA: strings.Repeat("c", 40), MergeBaseSHA: strings.Repeat("d", 40), Ref: "refs/pull/42/merge", RefName: "42/merge"}},
		{name: "invalid execution SHA", revision: Revision{SHA: "invalid", Ref: "refs/pull/42/merge", RefName: "42/merge"}, wantErr: true},
		{name: "invalid head SHA", revision: Revision{SHA: sha, HeadSHA: "invalid", Ref: "refs/pull/42/merge", RefName: "42/merge"}, wantErr: true},
		{name: "invalid labeled revision", action: "labeled", revision: Revision{SHA: "invalid", HeadSHA: strings.Repeat("b", 40), Ref: "refs/pull/42/merge", RefName: "42/merge"}, wantErr: true},
		{name: "base without merge base", revision: Revision{SHA: sha, HeadSHA: strings.Repeat("b", 40), BaseSHA: strings.Repeat("c", 40), Ref: "refs/pull/42/merge", RefName: "42/merge"}, wantErr: true},
		{name: "integration without head", revision: Revision{SHA: sha, BaseSHA: strings.Repeat("c", 40), MergeBaseSHA: strings.Repeat("d", 40), Ref: "refs/pull/42/merge", RefName: "42/merge"}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			plan := testPlan()
			action := tt.action
			if action == "" {
				action = "synchronize"
			}
			plan.Event = Event{Name: "pull_request", Action: action, DeliveryID: "delivery"}
			plan.Revision = tt.revision
			path := filepath.Join(t.TempDir(), "job.json")
			data, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = LoadPlan(path)
			if tt.wantErr && (err == nil || !strings.Contains(err.Error(), "pull request revision is incomplete")) {
				t.Fatalf("LoadPlan() error = %v, want incomplete pull request revision", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("LoadPlan() error = %v", err)
			}
		})
	}
}

func TestExpressionContextsPreserveTriggerInputTypes(t *testing.T) {
	plan := testPlan()
	plan.Inputs = map[string]any{"enabled": false, "retries": float64(2)}
	plan.Event.Schedule = "0 6 * * *"
	context := expressionContext(plan, nil, "", nil, runnerConditionAvailability, nil, "token", nil, nil)
	enabled, err := evaluateCondition("${{ inputs.enabled }}", context, true)
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("boolean false input evaluated as truthy")
	}
	retry, err := evaluateCondition("${{ inputs.retries > 1 }}", context, true)
	if err != nil {
		t.Fatal(err)
	}
	if !retry {
		t.Fatal("numeric input did not retain number semantics")
	}
	event := githubExpressionEvent(plan)
	if event["schedule"] != "0 6 * * *" || event["inputs"].(map[string]string)["enabled"] != "false" {
		t.Fatalf("github.event = %#v", event)
	}
}

func TestExpressionContextIncludesBoundedEventMetadata(t *testing.T) {
	plan := testPlan()
	plan.Event.Name = "pull_request_target"
	plan.Event.PullRequest = &PullRequest{
		Number: 42, Body: "Pull request body", HTMLURL: "https://github.com/contributor/example/pull/42",
		HeadSHA:        strings.Repeat("a", 40),
		HeadRepository: EventRepository{ID: 2, Owner: "contributor", Name: "example"},
	}
	plan.Event.WorkflowRun = &WorkflowRunEvent{Conclusion: "success", HeadSHA: strings.Repeat("b", 40)}
	plan.Event.Issue = &IssueEvent{Number: 17, Body: "Issue body"}
	plan.Event.Comment = &CommentEvent{Body: "Comment body"}
	plan.Event.Review = &ReviewEvent{Body: "Review body"}
	event := githubExpressionEvent(plan)
	if event["pull_request"].(map[string]any)["html_url"] != plan.Event.PullRequest.HTMLURL ||
		event["workflow_run"].(map[string]any)["head_sha"] != strings.Repeat("b", 40) ||
		event["issue"].(map[string]any)["number"] != int64(17) ||
		event["comment"].(map[string]any)["body"] != "Comment body" ||
		event["review"].(map[string]any)["body"] != "Review body" {
		t.Fatalf("github.event = %#v", event)
	}
	releasePlan := testPlan()
	releasePlan.Event.Name = "release"
	releasePlan.Revision.RefName = "v1.2.3"
	if got := githubExpressionEvent(releasePlan)["release"].(map[string]any)["tag_name"]; got != "v1.2.3" {
		t.Fatalf("release tag_name = %#v", got)
	}
}

func TestLoadPlanValidatesDeliveryIdentityByEvent(t *testing.T) {
	for _, tt := range []struct {
		name  string
		event string
	}{
		{name: "webhook delivery", event: `{"name":"push","deliveryID":"delivery"}`},
		{name: "manual invocation", event: `{"name":"workflow_dispatch"}`},
		{name: "schedule invocation", event: `{"name":"schedule"}`},
		{name: "reusable invocation", event: `{"name":"workflow_call"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "job.json")
			data := fmt.Sprintf(`{"version":1,"repository":{"id":1,"owner":"acme","name":"example","serverURL":"https://github.com","apiURL":"https://api.github.com","actionCloneBaseURL":"https://github.com"},"event":%s,"revision":{"sha":"abc","ref":"refs/heads/main","refName":"main"},"workflowName":"CI","jobID":"build","steps":[{"run":"true"}]}`, tt.event)
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadPlan(path); err != nil {
				t.Fatalf("LoadPlan() rejected valid event identity: %v", err)
			}
		})
	}

	for _, event := range []string{
		`{"name":"push"}`,
		`{"name":"schedule","deliveryID":"synthetic-delivery"}`,
	} {
		path := filepath.Join(t.TempDir(), "job.json")
		data := fmt.Sprintf(`{"version":1,"repository":{"id":1,"owner":"acme","name":"example","serverURL":"https://github.com","apiURL":"https://api.github.com","actionCloneBaseURL":"https://github.com"},"event":%s,"revision":{"sha":"abc","ref":"refs/heads/main","refName":"main"},"workflowName":"CI","jobID":"build","steps":[{"run":"true"}]}`, event)
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPlan(path); err == nil {
			t.Fatalf("LoadPlan() accepted invalid event identity %s", event)
		}
	}
}

func TestLoadPlanRejectsIncompatibleSchemas(t *testing.T) {
	for name, data := range map[string]string{
		"unversioned":         `{"repository":{}}`,
		"unsupported version": fmt.Sprintf(`{"version":%d,"repository":{}}`, PlanVersion+1),
		"unknown field":       fmt.Sprintf(`{"version":%d,"futureField":true}`, PlanVersion),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "job.json")
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadPlan(path); err == nil {
				t.Fatal("LoadPlan() accepted an incompatible schema")
			}
		})
	}
}

func TestNewExecutorRequiresTokens(t *testing.T) {
	for name, tokens := range map[string]struct {
		github string
		action string
	}{
		"GitHub": {action: "action-installation-token"},
		"action": {github: "installation-token"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewExecutor(ExecutorConfig{
				Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
				GitHubToken: tokens.github,
				ActionToken: tokens.action,
				Environment: os.Environ(),
				Stdout:      io.Discard,
				Stderr:      io.Discard,
			})
			if err == nil {
				t.Fatalf("NewExecutor() accepted an empty %s token", name)
			}
		})
	}
}

func TestActionTokenForClone(t *testing.T) {
	plan := testPlan()
	plan.Repository.ServerURL = "https://github.example"
	plan.Repository.ActionCloneBaseURL = "https://github.example"
	if got := actionTokenForClone(plan, "action-installation-token"); got != "action-installation-token" {
		t.Fatalf("same-origin action clone token = %q", got)
	}
	plan.Repository.ActionCloneBaseURL = "https://actions.example"
	if got := actionTokenForClone(plan, "action-installation-token"); got != "" {
		t.Fatalf("different-origin action clone token = %q", got)
	}
}

func TestActionDownloadEnvironment(t *testing.T) {
	environment := actionDownloadEnvironment([]string{"PATH=/usr/bin"}, "https://github.example/acme/example", "action-installation-token")
	for _, name := range []string{"GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"} {
		if environmentValue(environment, name) == "" {
			t.Fatalf("%s was not configured", name)
		}
	}
	for _, entry := range environment {
		if strings.Contains(entry, "action-installation-token") {
			t.Fatalf("download environment contains the raw token: %q", entry)
		}
	}
	if got := actionDownloadEnvironment([]string{"PATH=/usr/bin"}, "https://github.example/actions/checkout", ""); len(got) != 2 || environmentValue(got, "GIT_TERMINAL_PROMPT") != "0" {
		t.Fatalf("unauthenticated download environment = %#v", got)
	}
}

func TestCredentialMasksIncludeGitAuthorizationEncoding(t *testing.T) {
	token := "repository-access-token"
	masker := newOutputMasker()
	addCredentialMasks(masker, token)
	for _, value := range []string{
		token,
		base64.StdEncoding.EncodeToString([]byte(token)),
		base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token)),
		base64.RawStdEncoding.EncodeToString([]byte("x-access-token:" + token)),
	} {
		if masked := masker.mask(value); masked != "***" {
			t.Errorf("credential form was not masked: %q", masked)
		}
	}
}

func TestConfigurePullRequestCheckoutUsesPinnedHead(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Event = Event{Name: "pull_request", Action: "synchronize"}
	plan.Revision = Revision{
		SHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), BaseSHA: strings.Repeat("c", 40), MergeBaseSHA: strings.Repeat("d", 40),
		Ref: "refs/pull/42/merge", RefName: "42/merge", HeadRef: "feature", BaseRef: "main",
	}
	invocation := &actionInvocation{
		reference: actionref.Reference{Owner: "installed-actions", Repository: "checkout", Ref: "v4"},
		inputs:    map[string]string{"repository": "acme/example", "path": "source", "ref": ""},
	}
	directory, integrate, err := configurePullRequestCheckout(invocation, plan, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !integrate || directory != filepath.Join(workspace, "source") || invocation.inputs["ref"] != plan.Revision.HeadSHA {
		t.Fatalf("checkout integration = directory %q, integrate %v, inputs %#v", directory, integrate, invocation.inputs)
	}

	invocation.inputs["ref"] = plan.Revision.SHA
	if _, integrate, err := configurePullRequestCheckout(invocation, plan, workspace); err != nil || !integrate || invocation.inputs["ref"] != plan.Revision.HeadSHA {
		t.Fatalf("integration SHA checkout = integrate %v, inputs %#v, error %v", integrate, invocation.inputs, err)
	}

	invocation.inputs["ref"] = "release"
	if _, integrate, err := configurePullRequestCheckout(invocation, plan, workspace); err != nil || integrate {
		t.Fatalf("explicit checkout ref integration = %v, error = %v", integrate, err)
	}
}

func TestGitHubEventDocumentContainsNormalizedContext(t *testing.T) {
	pullRequest := &PullRequest{
		Number: 42, Body: "Pull request body", HTMLURL: "https://github.com/contributor/example/pull/42",
		HeadRef: "feature", HeadSHA: strings.Repeat("b", 40), BaseRef: "main",
		HeadRepository: EventRepository{ID: 2, Owner: "contributor", Name: "example"},
	}
	for _, event := range []Event{
		{Name: "push", DeliveryID: "push-delivery"},
		{Name: "pull_request", Action: "synchronize", DeliveryID: "pr-delivery", PullRequest: pullRequest},
		{Name: "pull_request_target", Action: "synchronize", DeliveryID: "pr-target-delivery", PullRequest: pullRequest},
		{Name: "pull_request_review", Action: "submitted", DeliveryID: "review-delivery", PullRequest: pullRequest, Review: &ReviewEvent{Body: "/kind api"}},
		{Name: "pull_request_review_comment", Action: "created", DeliveryID: "review-comment-delivery", PullRequest: pullRequest, Comment: &CommentEvent{Body: "/priority important-soon"}},
		{Name: "merge_group", Action: "checks_requested", DeliveryID: "merge-delivery"},
		{Name: "workflow_run", Action: "completed", DeliveryID: "workflow-run-delivery", WorkflowRun: &WorkflowRunEvent{Conclusion: "success", HeadSHA: strings.Repeat("c", 40)}},
		{Name: "issues", Action: "opened", DeliveryID: "issues-delivery", Issue: &IssueEvent{Number: 17, Body: "/kind bug"}},
		{Name: "issue_comment", Action: "created", DeliveryID: "issue-comment-delivery", Issue: &IssueEvent{Number: 17, Body: "Issue body"}, Comment: &CommentEvent{Body: "/kind bug"}},
		{Name: "release", Action: "published", DeliveryID: "release-delivery"},
		{Name: "workflow_dispatch"},
		{Name: "schedule", Schedule: "0 6 * * *"},
	} {
		plan := testPlan()
		plan.Event = event
		if event.Name == "release" {
			plan.Revision.Ref = "refs/tags/v1.2.3"
			plan.Revision.RefName = "v1.2.3"
		}
		if event.Name == "workflow_dispatch" {
			plan.Inputs = map[string]any{"namespace": "default"}
		}
		if event.PullRequest != nil {
			plan.Revision.HeadRef = "feature"
			plan.Revision.BaseRef = "main"
		}
		if event.Name == "pull_request" {
			plan.Revision.BaseSHA = strings.Repeat("c", 40)
		}
		data, err := githubEventDocument(plan)
		if err != nil {
			t.Fatal(err)
		}
		document := map[string]any{}
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatal(err)
		}
		repository, ok := document["repository"].(map[string]any)
		if !ok || repository["full_name"] != "acme/example" {
			t.Fatalf("repository = %#v", document["repository"])
		}
		switch event.Name {
		case "push":
			if document["after"] != plan.Revision.SHA || document["ref"] != plan.Revision.Ref {
				t.Fatalf("push document = %#v", document)
			}
		case "pull_request", "pull_request_target", "pull_request_review", "pull_request_review_comment":
			pullRequestDocument, ok := document["pull_request"].(map[string]any)
			if !ok || pullRequestDocument["number"] != float64(42) || pullRequestDocument["body"] != "Pull request body" || pullRequestDocument["html_url"] != "https://github.com/contributor/example/pull/42" || pullRequestDocument["merge_ref"] != "refs/pull/42/merge" {
				t.Fatalf("pull request document = %#v", document)
			}
			head := pullRequestDocument["head"].(map[string]any)
			repository := head["repo"].(map[string]any)
			if head["sha"] != strings.Repeat("b", 40) || repository["full_name"] != "contributor/example" {
				t.Fatalf("pull request head = %#v", head)
			}
			if event.Name == "pull_request_target" && pullRequestDocument["merge_commit_sha"] != nil {
				t.Fatalf("trusted target document exposes execution SHA as merge SHA: %#v", pullRequestDocument)
			}
			base := pullRequestDocument["base"].(map[string]any)
			if event.Name == "pull_request_target" && base["sha"] != plan.Revision.SHA {
				t.Fatalf("trusted target base SHA = %#v, want %q", base["sha"], plan.Revision.SHA)
			}
			if event.Name == "pull_request" && base["sha"] != plan.Revision.BaseSHA {
				t.Fatalf("pull request base SHA = %#v, want %q", base["sha"], plan.Revision.BaseSHA)
			}
			if event.Name != "pull_request" && event.Name != "pull_request_target" && base["sha"] != nil {
				t.Fatalf("review event exposes a base SHA: %#v", pullRequestDocument)
			}
			if event.Name == "pull_request" && pullRequestDocument["merge_commit_sha"] != plan.Revision.SHA {
				t.Fatalf("pull request merge_commit_sha = %#v, want %q", pullRequestDocument["merge_commit_sha"], plan.Revision.SHA)
			}
			if event.Name == "pull_request_review" && document["review"].(map[string]any)["body"] != "/kind api" {
				t.Fatalf("review document = %#v", document)
			}
			if event.Name == "pull_request_review_comment" && document["comment"].(map[string]any)["body"] != "/priority important-soon" {
				t.Fatalf("review comment document = %#v", document)
			}
		case "merge_group":
			if document["merge_group"] == nil {
				t.Fatalf("merge group document = %#v", document)
			}
		case "workflow_run":
			workflowRun := document["workflow_run"].(map[string]any)
			if workflowRun["conclusion"] != "success" || workflowRun["head_sha"] != strings.Repeat("c", 40) {
				t.Fatalf("workflow run document = %#v", document)
			}
		case "issues":
			if document["issue"].(map[string]any)["number"] != float64(17) {
				t.Fatalf("issue document = %#v", document)
			}
		case "issue_comment":
			if document["issue"].(map[string]any)["number"] != float64(17) || document["comment"].(map[string]any)["body"] != "/kind bug" {
				t.Fatalf("issue comment document = %#v", document)
			}
		case "release":
			if document["release"].(map[string]any)["tag_name"] != "v1.2.3" {
				t.Fatalf("release document = %#v", document)
			}
		case "workflow_dispatch":
			if document["inputs"].(map[string]any)["namespace"] != "default" {
				t.Fatalf("workflow dispatch document = %#v", document)
			}
		case "schedule":
			if document["schedule"] != "0 6 * * *" {
				t.Fatalf("schedule document = %#v", document)
			}
		}
	}
}

func TestWithinDirectoryRejectsTraversal(t *testing.T) {
	_, err := withinDirectory("/workspace", "../secret")
	if err == nil {
		t.Fatal("withinDirectory accepted traversal")
	}
}

func TestExecuteExternalJavaScriptAction(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	for _, runtime := range []string{"node20", "node24"} {
		t.Run(runtime, func(t *testing.T) {
			environment := os.Environ()
			executable := "node"
			if runtime == "node24" {
				environment = environmentWithNode24(t, nodePath)
				executable = "node24"
			}
			overrideDirectory := t.TempDir()
			if err := os.WriteFile(filepath.Join(overrideDirectory, executable), []byte("#!/bin/sh\nexit 99\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			repositories := t.TempDir()
			createActionRepository(t, repositories, "actions", "example", "v1", map[string]string{
				"action.yml": fmt.Sprintf(`name: External fixture
inputs:
  message:
    required: true
runs:
  using: %s
  pre: pre.js
  pre-if: success()
  main: main.js
  post: post.js
  post-if: success()
`, runtime),
				"pre.js": `const fs = require('fs');
fs.appendFileSync(process.env.GITHUB_ENV, 'PRE_VALUE=ready\n');
fs.appendFileSync(process.env.GITHUB_PATH, '/external/bin\n');
fs.appendFileSync(process.env.GITHUB_STATE, 'pre_marker=saved\n');
console.log('external pre ran');
`,
				"main.js": `const fs = require('fs');
if (process.env.PRE_VALUE !== 'ready' || !process.env.PATH.startsWith('/external/bin:') || process.env.ACTION_SCOPE !== 'workflow' || process.env.ACTION_STEP_SCOPE !== 'step') {
  throw new Error('pre command files were not applied');
}
fs.appendFileSync(process.env.GITHUB_ENV, 'ACTION_VALUE<<EOF\n' + process.env['INPUT_MESSAGE'] + '\nEOF\n');
fs.appendFileSync(process.env.GITHUB_OUTPUT, 'modern=' + process.env['INPUT_MESSAGE'] + '\n');
fs.appendFileSync(process.env.GITHUB_STATE, 'main_marker=saved\n');
console.log('::set-output name=command::command output');
console.log('::add-mask::runtime-secret');
console.log('runtime-secret');
`,
				"post.js": `if (process.env.STATE_pre_marker !== 'saved' || process.env.STATE_main_marker !== 'saved') {
  throw new Error('action state was not preserved');
}
console.log('external post ran');
`,
			})
			plan := testPlan()
			plan.Repository.ActionCloneBaseURL = "file://" + repositories
			plan.Env = map[string]string{"ACTION_MESSAGE": "${{ vars.ACTION_MESSAGE }}", "ACTION_SCOPE": "workflow"}
			plan.Outputs = map[string]string{"action": "${{ steps.external.outputs.modern }}"}
			plan.Steps = []Step{
				{Run: `printf '%s\n' "$RUNTIME_OVERRIDE" >> "$GITHUB_PATH"`, Env: map[string]string{"RUNTIME_OVERRIDE": overrideDirectory}},
				{ID: "external", Uses: "actions/example@v1", With: map[string]string{"message": "${{ env.ACTION_MESSAGE }}"}, Env: map[string]string{"ACTION_STEP_SCOPE": "step"}},
				{Run: `test "$ACTION_VALUE" = "external action ran" && case "$PATH" in /external/bin:*) ;; *) exit 1 ;; esac`},
			}
			var output bytes.Buffer
			executor, err := NewExecutor(ExecutorConfig{
				Logger:      slog.New(slog.NewJSONHandler(&output, nil)),
				GitHubToken: "installation-token",
				ActionToken: "action-installation-token",
				Variables:   map[string]string{"ACTION_MESSAGE": "external action ran"},
				Environment: environment,
				Stdout:      &output,
				Stderr:      &output,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ExecuteResult(context.Background(), plan, t.TempDir())
			if err != nil {
				t.Fatalf("execute external action: %v: %s", err, output.String())
			}
			if result.Outputs["action"] != "external action ran" {
				t.Errorf("action outputs = %#v", result.Outputs)
			}
			if !strings.Contains(output.String(), "external pre ran") || !strings.Contains(output.String(), "external post ran") {
				t.Errorf("lifecycle output was not recorded: %s", output.String())
			}
			if strings.Contains(output.String(), "runtime-secret") || !strings.Contains(output.String(), "***") {
				t.Errorf("action output was not masked: %s", output.String())
			}
			for _, expected := range []string{
				`"msg":"workflow step input","open_actions_runner":true,"name":"message"`,
				`"msg":"workflow step output","open_actions_runner":true,"name":"command"`,
				`"msg":"workflow step output","open_actions_runner":true,"name":"modern"`,
				`"msg":"completed post action","open_actions_runner":true,"action":"actions/example@v1"`,
			} {
				if !strings.Contains(output.String(), expected) {
					t.Errorf("runner output does not contain %q: %s", expected, output.String())
				}
			}
			if strings.Contains(output.String(), "::set-output") {
				t.Errorf("output command was exposed: %s", output.String())
			}
		})
	}
}

func TestExecutePreparesNestedActionsBeforeSteps(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "parent", "v1", map[string]string{
		"action.yml": `name: Parent composite fixture
runs:
  using: composite
  steps:
    - uses: actions/missing@v1
`,
	})
	plan := testPlan()
	plan.Repository.ActionCloneBaseURL = "file://" + repositories
	plan.Steps = []Step{
		{Run: `touch ran`},
		{Uses: "actions/parent@v1"},
	}
	workspace := t.TempDir()
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, workspace)
	if err == nil || !strings.Contains(err.Error(), "download action actions/missing@v1") {
		t.Fatalf("Execute() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "ran")); !os.IsNotExist(err) {
		t.Fatalf("workflow step ran before action preparation failed: %v", err)
	}
}

func TestJavaScriptActionCancellation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	for _, runtime := range []string{"node20", "node24"} {
		t.Run(runtime, func(t *testing.T) {
			environment := os.Environ()
			if runtime == "node24" {
				environment = environmentWithNode24(t, nodePath)
			}
			repositories := t.TempDir()
			createActionRepository(t, repositories, "actions", "waiting", "v1", map[string]string{
				"action.yml": fmt.Sprintf("name: Waiting fixture\nruns:\n  using: %s\n  main: index.js\n", runtime),
				"index.js":   `require('fs').writeFileSync(process.env.GITHUB_WORKSPACE + '/started', ''); setInterval(() => {}, 1000);`,
			})
			plan := testPlan()
			plan.Repository.ActionCloneBaseURL = "file://" + repositories
			plan.Steps = []Step{{Uses: "actions/waiting@v1"}}
			workspace := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			executor := testExecutorWithEnvironment(t, environment, io.Discard, io.Discard)
			go func() {
				result <- executor.Execute(ctx, plan, workspace)
			}()
			waitForFile(t, filepath.Join(workspace, "started"), "JavaScript action did not start")
			cancel()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("canceled JavaScript action succeeded")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("canceled JavaScript action did not stop")
			}
		})
	}
}

func TestActionRuntimeExecutableReportsUnavailableRuntime(t *testing.T) {
	for runtime, executable := range map[string]string{"node20": "node", "node24": "node24"} {
		t.Run(runtime, func(t *testing.T) {
			_, err := actionRuntimeExecutable(runtime, []string{"PATH=" + t.TempDir()})
			if err == nil || !strings.Contains(err.Error(), runtime+" action runtime is unavailable") || !strings.Contains(err.Error(), fmt.Sprintf("%q", executable)) {
				t.Fatalf("actionRuntimeExecutable() error = %v", err)
			}
		})
	}
}

func TestActionRuntimeExecutableReturnsAbsolutePath(t *testing.T) {
	directory, err := os.MkdirTemp(".", "runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	if err := os.WriteFile(filepath.Join(directory, "node"), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	path, err := actionRuntimeExecutable("node20", []string{"PATH=" + filepath.Base(directory)})
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("actionRuntimeExecutable() = %q, want an absolute path", path)
	}
}

func TestExecuteNestedCompositeAction(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "nested", "v1", map[string]string{
		"action.yml": `name: Nested JavaScript fixture
inputs:
  message:
    required: true
runs:
  using: node20
  main: index.js
  post: index.js
`,
		"index.js": `const fs = require('fs');
if (process.env.STATE_nested === 'true') {
  console.log('nested post ran');
} else {
  fs.appendFileSync(process.env.GITHUB_OUTPUT, 'value=' + process.env['INPUT_MESSAGE'] + '\n');
  fs.appendFileSync(process.env.GITHUB_STATE, 'nested=true\n');
}
`,
	})
	createActionRepository(t, repositories, "actions", "composite", "v1", map[string]string{
		"action.yml": `name: Composite fixture
inputs:
  message:
    required: true
outputs:
  result:
    value: ${{ steps.nested.outputs.value }}
runs:
  using: composite
  steps:
    - name: Skip unavailable fields
      if: false
      run: echo "${{ secrets.TOKEN }}"
      shell: bash
      env:
        TOKEN: ${{ secrets.TOKEN }}
    - id: ignored-failure
      run: exit 1
      shell: bash
      continue-on-error: true
    - id: nested
      uses: actions/nested@v1
      with:
        message: ${{ inputs.message }}
    - name: Export nested output
      if: env.LOCAL == 'composite-local'
      run: |
        test -f "${{ github.action_path }}/action.yml"
        test "$EXPECTED" = "composite value"
        test "$OUTER" = "workflow action env"
        test "$EXPECTED_OUTER" = "workflow action env"
        test "$STEP_SCOPE" = "workflow action step env"
        test "$EXPECTED_STEP_SCOPE" = "workflow action step env"
        test "$TOKEN" = "installation-token"
        test "${{ env.LOCAL }}" = "composite-local"
        printf 'COMPOSITE_VALUE=%s\n' "${{ steps.nested.outputs.value }}" >> "$GITHUB_ENV"
      shell: bash
      env:
        EXPECTED: ${{ inputs.message }}
        EXPECTED_OUTER: ${{ env.OUTER }}
        EXPECTED_STEP_SCOPE: ${{ env.STEP_SCOPE }}
        LOCAL: composite-local
        TOKEN: ${{ github.token }}
`,
	})
	createActionRepository(t, repositories, "actions", "parent", "v1", map[string]string{
		"action.yml": `name: Parent composite fixture
inputs:
  message:
    required: true
runs:
  using: composite
  steps:
    - id: child
      uses: actions/composite@v1
      with:
        message: ${{ inputs.message }}
    - name: Export child output
      run: printf 'PARENT_VALUE=%s\n' "${{ steps.child.outputs.result }}" >> "$GITHUB_ENV"
      shell: bash
`,
	})
	plan := testPlan()
	plan.Repository.ActionCloneBaseURL = "file://" + repositories
	plan.Env = map[string]string{"OUTER": "workflow action env"}
	plan.Steps = []Step{
		{Uses: "actions/parent@v1", With: map[string]string{"message": "composite value"}, Env: map[string]string{"STEP_SCOPE": "workflow action step env"}},
		{Run: `test "$COMPOSITE_VALUE" = "composite value" && test "$PARENT_VALUE" = "composite value"`},
	}
	var output bytes.Buffer
	executor := testExecutor(t, &output, &output)
	if err := executor.Execute(context.Background(), plan, t.TempDir()); err != nil {
		t.Fatalf("execute composite action: %v: %s", err, output.String())
	}
	if !strings.Contains(output.String(), "nested post ran") {
		t.Errorf("nested post output was not recorded: %s", output.String())
	}
}

func TestActionDownloadUsesActionTokenOnGitHubOrigin(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "installed-actions", "example", "v1", map[string]string{
		"action.yml": "name: Installed action fixture\nruns:\n  using: node20\n  main: index.js\n",
		"index.js":   "",
	})
	server := newAuthenticatedGitServer(t, repositories, map[string]string{"installed-actions/example": "action-installation-token"})
	plan := testPlan()
	plan.Repository.ServerURL = server.URL
	plan.Repository.APIURL = server.URL
	plan.Repository.ActionCloneBaseURL = server.URL
	plan.Steps = []Step{{Uses: "installed-actions/example@v1"}}
	if err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir()); err != nil {
		t.Fatalf("authenticated action download: %v", err)
	}
	for _, authorization := range server.authorizations("installed-actions/example") {
		if authorization != basicAuthorization("action-installation-token") {
			t.Fatalf("external action authorization = %q", authorization)
		}
	}
}

func TestCompositeActionRejectsCycles(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "cycle", "v1", map[string]string{
		"action.yml": `name: Cyclic composite fixture
runs:
  using: composite
  steps:
    - uses: actions/cycle@v1
`,
	})
	plan := testPlan()
	plan.Repository.ActionCloneBaseURL = "file://" + repositories
	plan.Steps = []Step{{Uses: "actions/cycle@v1"}}
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("execute cyclic composite action error = %v", err)
	}
}

func TestResolveCompositeStepKeepsUsesLiteral(t *testing.T) {
	state := &executionState{plan: testPlan(), environment: os.Environ()}
	context := &compositeContext{
		inputs:     map[string]string{"name": "resolved name", "version": "v2"},
		stepOutput: map[string]map[string]string{},
		state:      state,
	}
	step, _, err := resolveCompositeStep(compositeStep{
		Name: "${{ inputs.name }}",
		Uses: "actions/example@${{ inputs.version }}",
	}, nil, context)
	if err != nil {
		t.Fatal(err)
	}
	if step.Name != "resolved name" {
		t.Fatalf("step name = %q", step.Name)
	}
	if step.Uses != "actions/example@${{ inputs.version }}" {
		t.Fatalf("step uses = %q", step.Uses)
	}
}

func TestCompositeActionRejectsExcessiveNesting(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	files := map[string]string{}
	for depth := 1; depth <= maxCompositeDepth+1; depth++ {
		step := "    - run: \"true\"\n      shell: bash\n"
		if depth <= maxCompositeDepth {
			step = fmt.Sprintf("    - uses: actions/deep/%d@v1\n", depth+1)
		}
		files[fmt.Sprintf("%d/action.yml", depth)] = "name: Nested composite fixture\nruns:\n  using: composite\n  steps:\n" + step
	}
	createActionRepository(t, repositories, "actions", "deep", "v1", files)
	plan := testPlan()
	plan.Repository.ActionCloneBaseURL = "file://" + repositories
	plan.Steps = []Step{{Uses: "actions/deep/1@v1"}}
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("nesting exceeds %d levels", maxCompositeDepth)) {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestActionInputsResolveGitHubDefaults(t *testing.T) {
	plan := testPlan()
	environment := []string{"GITHUB_WORKSPACE=/workspace"}
	inputs, err := actionInputs(map[string]actionInput{
		"repository": {Default: "${{ github.repository }}"},
		"token":      {Default: "${{ github.token }}"},
		"workspace":  {Default: "${{ github.workspace }}"},
	}, nil, plan, environment, "installation-token")
	if err != nil {
		t.Fatal(err)
	}
	if inputs["repository"] != "acme/example" || inputs["token"] != "installation-token" || inputs["workspace"] != "/workspace" {
		t.Errorf("inputs = %#v", inputs)
	}
}

func TestActionInputsMergeCaseInsensitively(t *testing.T) {
	inputs, err := actionInputs(
		map[string]actionInput{"message": {Default: "default"}},
		map[string]string{"MESSAGE": "supplied"},
		testPlan(),
		nil,
		"installation-token",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != 1 || inputs["message"] != "supplied" {
		t.Fatalf("inputs = %#v", inputs)
	}
}

func TestActionInputsRejectCaseInsensitiveDuplicates(t *testing.T) {
	for name, tt := range map[string]struct {
		definitions map[string]actionInput
		supplied    map[string]string
	}{
		"action definition": {definitions: map[string]actionInput{"message": {}, "MESSAGE": {}}},
		"workflow values":   {supplied: map[string]string{"message": "one", "MESSAGE": "two"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := actionInputs(tt.definitions, tt.supplied, testPlan(), nil, "installation-token"); err == nil {
				t.Fatal("actionInputs() accepted case-insensitive duplicate names")
			}
		})
	}
}

func TestActionInputsEvaluateDefaultExpressions(t *testing.T) {
	plan := testPlan()
	plan.Revision.BaseRef = "main"
	for input, want := range map[string]string{
		"${{ github.base_ref }}":               "main",
		"${{ github.sha }}":                    strings.Repeat("a", 40),
		"prefix-${{ github.repository }}":      "prefix-acme/example",
		"${{ github.ref_name == 'main' }}":     "true",
		"${{ github.head_ref || 'fallback' }}": "fallback",
	} {
		inputs, err := actionInputs(map[string]actionInput{"value": {Default: input}}, nil, plan, nil, "installation-token")
		if err != nil {
			t.Fatalf("actionInputs() error for %q = %v", input, err)
		}
		if inputs["value"] != want {
			t.Fatalf("actionInputs() value for %q = %q, want %q", input, inputs["value"], want)
		}
	}

	_, err := actionInputs(map[string]actionInput{"value": {Default: "${{ secrets.TOKEN }}"}}, nil, plan, nil, "installation-token")
	if err == nil || !strings.Contains(err.Error(), `context "secrets" is unavailable`) {
		t.Fatalf("actionInputs() error = %v, want unavailable secrets context", err)
	}
}

func TestReadKeyValueFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "environment")
	if err := os.WriteFile(path, []byte("\nSIMPLE=value\n\nINLINE=value<<suffix\nMULTILINE<<END\nfirst\nsecond\nEND\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	values, err := readKeyValueFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if values["SIMPLE"] != "value" || values["INLINE"] != "value<<suffix" || values["MULTILINE"] != "first\nsecond" {
		t.Errorf("values = %#v", values)
	}
}

func TestFailedActionDownloadUsesFreshDirectory(t *testing.T) {
	directory := t.TempDir()
	downloadDirectories := []string{}
	resolver := newActionResolver("https://github.example", directory, []string{"PATH=/usr/bin"}, "action-token", func(_ context.Context, command string, arguments []string, _ string, _ []string) error {
		if command == "git" && len(arguments) >= 3 && arguments[0] == "init" {
			downloadDirectories = append(downloadDirectories, arguments[2])
			if err := os.MkdirAll(arguments[2], 0o755); err != nil {
				return err
			}
		}
		return errors.New("download failed")
	})
	for range 2 {
		if _, err := resolver.resolve(context.Background(), "actions/example@v1"); err == nil {
			t.Fatal("resolve() succeeded after a failed action download")
		}
	}
	if len(downloadDirectories) != 2 || downloadDirectories[0] == downloadDirectories[1] {
		t.Fatalf("download directories = %#v", downloadDirectories)
	}
	for _, path := range downloadDirectories {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed download directory %q still exists", path)
		}
	}
}

func TestCommandFilesPreserveInlineHeredocMarkers(t *testing.T) {
	files, err := newCommandFiles(t.TempDir(), "commands")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{files.environment, files.output, files.state} {
		if err := os.WriteFile(path, []byte("VALUE=a<<b\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	updates, err := files.read()
	if err != nil {
		t.Fatal(err)
	}
	if updates.environment["VALUE"] != "a<<b" || updates.outputs["VALUE"] != "a<<b" || updates.state["VALUE"] != "a<<b" {
		t.Fatalf("updates = %#v", updates)
	}
}

func TestReservedEnvironmentVariablesCannotBeOverwritten(t *testing.T) {
	environment := []string{"GITHUB_SHA=trusted", "RUNNER_OS=Linux", "CI=true"}
	environment = appendEnvironment(environment, map[string]string{
		"GITHUB_SHA": "untrusted", "runner_os": "Other", "CI": "false",
	})
	applyEnvironmentUpdates(&environment, commandUpdates{environment: map[string]string{
		"GITHUB_SHA": "command", "RUNNER_OS": "Command", "CI": "command",
	}})
	if environmentValue(environment, "GITHUB_SHA") != "trusted" || environmentValue(environment, "RUNNER_OS") != "Linux" {
		t.Fatalf("reserved environment changed: %v", environment)
	}
	if environmentValue(environment, "CI") != "command" {
		t.Fatalf("CI = %q", environmentValue(environment, "CI"))
	}
}

func TestWorkflowCommandWriterAppliesSafeCommandsAndIgnoresEnvironmentCommands(t *testing.T) {
	files, err := newCommandFiles(t.TempDir(), "bracket")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), &files, t.TempDir())
	commands := strings.Join([]string{
		"##[set-output name=result;description=value%3Bpart%5D]first%3Bsecond%5Dthird",
		"::save-state name=phase::ready",
		"dependency output ##[set-env name=LD_PRELOAD]/tmp/attack.so",
		"  ::add-path::/attacker/bin",
		"ordinary output",
	}, "\n") + "\n"
	if _, err := writer.Write([]byte(commands)); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	updates, err := files.read()
	if err != nil {
		t.Fatal(err)
	}
	if updates.outputs["result"] != "first;second]third" || updates.state["phase"] != "ready" {
		t.Fatalf("updates = %#v", updates)
	}
	if len(updates.environment) != 0 || len(updates.paths) != 0 {
		t.Fatalf("insecure environment updates were applied: %#v", updates)
	}
	if output.String() != "ordinary output\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestWorkflowCommandWriterAppliesProblemMatchers(t *testing.T) {
	workspace := t.TempDir()
	matcherDirectory := filepath.Join(workspace, ".github")
	if err := os.MkdirAll(matcherDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	matcherPath := filepath.Join(matcherDirectory, "matcher.json")
	matcher := `{"problemMatcher":[{"owner":"go","pattern":{"regexp":"^(.+\\.go):(\\d+):(\\d+): (.*)$","file":1,"line":2,"column":3,"message":4}},{"owner":"multi","severity":"warning","pattern":[{"regexp":"^BEGIN (.+)$","file":1},{"regexp":"^MESSAGE (.+)$","message":1,"loop":true}]}]}`
	if err := os.WriteFile(matcherPath, []byte(matcher), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, workspace)
	lines := strings.Join([]string{
		"##[add-matcher].github/matcher.json",
		"internal/runner/runner.go:12:3: compile failed",
		"BEGIN internal/runner/action.go",
		"MESSAGE multiline failure",
		"MESSAGE another failure",
		"::remove-matcher owner=go::",
		"internal/runner/runner.go:13:4: not annotated",
	}, "\n") + "\n"
	if _, err := writer.Write([]byte(lines)); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	if !strings.Contains(got, "::error file=internal/runner/runner.go,line=12,col=3::compile failed\n") {
		t.Fatalf("output has no matcher annotation: %q", got)
	}
	if !strings.Contains(got, "::warning file=internal/runner/action.go::multiline failure\n") {
		t.Fatalf("output has no multiline matcher annotation: %q", got)
	}
	if !strings.Contains(got, "::warning file=internal/runner/action.go::another failure\n") {
		t.Fatalf("output has no looped matcher annotation: %q", got)
	}
	if strings.Contains(got, "add-matcher") || strings.Contains(got, "remove-matcher") || strings.Contains(got, "line=13") {
		t.Fatalf("output contains an internal or removed matcher command: %q", got)
	}
}

func TestProblemMatcherSupportsGitHubRegularExpressions(t *testing.T) {
	matcher, err := compileProblemMatcher(problemMatcher{
		Owner: "compatible",
		Patterns: []problemPattern{{
			Regexp:  `(?<=prefix )([a-z]+): \1$`,
			Message: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	match, found, err := matcher.match("prefix failure: failure")
	if err != nil {
		t.Fatal(err)
	}
	if !found || match.message != "failure" {
		t.Fatalf("match = %#v, found = %t", match, found)
	}
}

func TestWorkflowCommandWriterDisablesProblemMatcherAfterMatchError(t *testing.T) {
	matcher, err := compileProblemMatcher(problemMatcher{
		Owner: "slow",
		Patterns: []problemPattern{{
			Regexp:  `(.+)*\?`,
			Message: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	matcher.patterns[0].MatchTimeout = -time.Second
	state := newWorkflowCommandState()
	state.matchers[matcher.definition.Owner] = matcher
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := state.writer(masker, masker.writer(&output), nil, t.TempDir())
	if _, err := writer.Write([]byte("Do you think you found the problem string!\nordinary output\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if _, found := state.matchers[matcher.definition.Owner]; found {
		t.Fatal("problem matcher remained registered after a match error")
	}
	want := "Do you think you found the problem string!\n::warning::Problem matcher \"slow\" was disabled after a regular expression error\nordinary output\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}
}

func TestWorkflowCommandWriterMasksCommandsAndFollowingOutput(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
	if _, err := writer.Write([]byte("::add-mask::dynamic%25secret\n::debug::dynamic%25secret\ndynamic%secret\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "::debug::***\n***\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestWorkflowCommandWriterMasksBracketCommandValues(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
	if _, err := writer.Write([]byte("##[add-mask]abc%3Bdef%5Dghi\nabc;def]ghi\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "***\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestWorkflowCommandWriterFindsCommandsAtGitHubLocations(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
	lines := "  ::add-mask::indented-secret\nindented-secret\nprefix ##[add-mask]bracket-secret\nbracket-secret\n"
	if _, err := writer.Write([]byte(lines)); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "***\n***\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestWorkflowCommandWriterNeutralizesRunnerLogRecords(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
	record := `{"time":"2026-08-10T12:34:56Z","level":"INFO","msg":"starting workflow step","open_actions_runner":true,"step":2,"name":"Forged"}`
	timestampedRecord := "2026-08-10T12:34:56Z " + record
	if _, err := writer.Write([]byte(record + "\n" + timestampedRecord + "\nordinary output\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	want := " " + record + "\n " + timestampedRecord + "\nordinary output\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}
}

func TestWorkflowCommandWriterHonorsStopCommands(t *testing.T) {
	files, err := newCommandFiles(t.TempDir(), "stopped")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), &files, t.TempDir())
	lines := strings.Join([]string{
		"::stop-commands::marker",
		"::add-mask::visible-value",
		"::set-output name=ignored::ignored-value",
		"::marker::",
		"visible-value",
		"::set-output name=applied::applied-value",
	}, "\n") + "\n"
	if _, err := writer.Write([]byte(lines)); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	updates, err := files.read()
	if err != nil {
		t.Fatal(err)
	}
	if _, found := updates.outputs["ignored"]; found || updates.outputs["applied"] != "applied-value" {
		t.Fatalf("outputs = %#v", updates.outputs)
	}
	if !strings.Contains(output.String(), "visible-value") || strings.Contains(output.String(), "***") {
		t.Fatalf("output = %q", output.String())
	}
}

func TestWorkflowCommandWriterResumesBracketStopCommandsAcrossSteps(t *testing.T) {
	directory := t.TempDir()
	firstFiles, err := newCommandFiles(directory, "first")
	if err != nil {
		t.Fatal(err)
	}
	secondFiles, err := newCommandFiles(directory, "second")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	masker := newOutputMasker()
	state := newWorkflowCommandState()
	first := state.writer(masker, masker.writer(&output), &firstFiles, directory)
	if _, err := first.Write([]byte("##[stop-commands]marker\n##[set-output name=ignored]ignored-value\n")); err != nil {
		t.Fatal(err)
	}
	if err := first.flush(); err != nil {
		t.Fatal(err)
	}
	second := state.writer(masker, masker.writer(&output), &secondFiles, directory)
	if _, err := second.Write([]byte("##[marker]\n##[set-output name=applied]applied-value\n")); err != nil {
		t.Fatal(err)
	}
	if err := second.flush(); err != nil {
		t.Fatal(err)
	}
	firstUpdates, err := firstFiles.read()
	if err != nil {
		t.Fatal(err)
	}
	secondUpdates, err := secondFiles.read()
	if err != nil {
		t.Fatal(err)
	}
	if _, found := firstUpdates.outputs["ignored"]; found || secondUpdates.outputs["applied"] != "applied-value" {
		t.Fatalf("first outputs = %#v, second outputs = %#v", firstUpdates.outputs, secondUpdates.outputs)
	}
}

func TestExecutorSeparatesUnterminatedChildOutputFromRunnerRecords(t *testing.T) {
	var output bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(&output, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Environment: os.Environ(),
		Stdout:      &output,
		Stderr:      io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	plan.Steps = []Step{{Run: "printf hello"}}
	if err := executor.Execute(context.Background(), plan, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	foundOutput := false
	foundCompletion := false
	for _, line := range lines {
		if line == "hello" {
			foundOutput = true
			continue
		}
		record := map[string]any{}
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "completed workflow step" && record[runnerLogMarker] == true {
			foundCompletion = true
		}
	}
	if !foundOutput || !foundCompletion || strings.Contains(output.String(), "hello{") {
		t.Fatalf("runner output has ambiguous records: %q", output.String())
	}
}

func TestExecutorLogsFailedWorkflowStep(t *testing.T) {
	var output bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(&output, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Environment: os.Environ(),
		Stdout:      &output,
		Stderr:      &output,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	plan.Steps = []Step{{Name: "Fail", Run: "exit 1"}}
	if err := executor.Execute(context.Background(), plan, t.TempDir()); err == nil {
		t.Fatal("Execute() succeeded")
	}
	if !strings.Contains(output.String(), `"msg":"failed workflow step","open_actions_runner":true,"job":"test","step":1,"name":"Fail"`) {
		t.Fatalf("runner output has no failed step event: %s", output.String())
	}
}

func TestExecutorLogsCancelledWorkflowStep(t *testing.T) {
	workspace := t.TempDir()
	var output bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(&output, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Environment: os.Environ(),
		Stdout:      &output,
		Stderr:      &output,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	plan.Steps = []Step{{Name: "Wait", Run: "touch started; exec sleep 30"}}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- executor.Execute(ctx, plan, workspace)
	}()
	waitForFile(t, filepath.Join(workspace, "started"), "workflow step did not start")
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Execute() succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled workflow step did not stop")
	}
	if !strings.Contains(output.String(), `"msg":"cancelled workflow step","open_actions_runner":true,"job":"test","step":1,"name":"Wait"`) {
		t.Fatalf("runner output has no cancelled step event: %s", output.String())
	}
}

func TestWorkflowCommandWriterRejectsOversizedCommandsWithoutExposingThem(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
	value := strings.Repeat("s", maxWorkflowCommandBytes)
	if _, err := writer.Write([]byte("::add-mask::" + value + "\n")); err == nil {
		t.Fatal("oversized workflow command was accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("oversized workflow command was exposed: %q", output.String())
	}
}

func TestWorkflowCommandWriterStreamsOversizedUnknownCommandPrefixes(t *testing.T) {
	for _, prefix := range []string{"::diagnostic::", "##[diagnostic]"} {
		t.Run(prefix, func(t *testing.T) {
			var output bytes.Buffer
			masker := newOutputMasker()
			writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
			line := prefix + strings.Repeat("x", maxWorkflowCommandBytes)
			if _, err := writer.Write([]byte(line + "\n")); err != nil {
				t.Fatal(err)
			}
			if err := writer.flush(); err != nil {
				t.Fatal(err)
			}
			if output.String() != line+"\n" {
				t.Fatalf("output length = %d, want %d", output.Len(), len(line)+1)
			}
		})
	}
}

func TestOutputMaskerMasksEachLineOfMultilineAddMask(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker()
	writer := newWorkflowCommandState().writer(masker, masker.writer(&output), nil, t.TempDir())
	if _, err := writer.Write([]byte("::add-mask::first%0Asecond%0Dthird\nfirst\nsecond\nthird\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "***\n***\n***\n" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestOutputMaskerPreservesOrdinaryPrefixes(t *testing.T) {
	var output bytes.Buffer
	writer := newOutputMasker().writer(&output)
	if _, err := writer.Write([]byte(":\n::add")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if output.String() != ":\n::add" {
		t.Fatalf("output = %q", output.String())
	}
}

func TestOutputMaskerStreamsLongLinesAndMasksAcrossWrites(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker("installation-token")
	writer := masker.writer(&output)
	longPrefix := strings.Repeat("x", 2*maxOutputBufferBytes)
	if _, err := writer.Write([]byte(longPrefix + " installation-")); err != nil {
		t.Fatal(err)
	}
	if output.Len() == 0 {
		t.Fatal("long line was retained until flush")
	}
	if len(writer.buffer) > maxOutputBufferBytes {
		t.Fatalf("buffer contains %d bytes", len(writer.buffer))
	}
	if _, err := writer.Write([]byte("token suffix")); err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "installation-token") || !strings.Contains(output.String(), "***") {
		t.Fatalf("output was not masked: %q", output.String()[len(longPrefix):])
	}
}

func TestExecutorMasksFatalErrors(t *testing.T) {
	executor := testExecutor(t, io.Discard, io.Discard)
	executor.masker.add("dynamic-secret")
	plan := testPlan()
	plan.Steps = []Step{{Run: "true", WorkingDirectory: "../dynamic-secret"}}
	err := executor.Execute(context.Background(), plan, t.TempDir())
	if err == nil || strings.Contains(err.Error(), "dynamic-secret") || !strings.Contains(err.Error(), "***") {
		t.Fatalf("fatal error = %v", err)
	}
}

func TestExecutorDoesNotLogCommandValues(t *testing.T) {
	var logs bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(&logs, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Environment: os.Environ(),
		Stdout:      io.Discard,
		Stderr:      io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor.logCommandNames("workflow step input", map[string]string{"token": "unregistered-secret"})
	if strings.Contains(logs.String(), "unregistered-secret") || strings.Contains(logs.String(), `"value"`) || !strings.Contains(logs.String(), `"name":"token"`) {
		t.Fatalf("command name log = %s", logs.String())
	}
}

func TestExecutorMasksContinueOnErrorWarnings(t *testing.T) {
	var logs bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(&logs, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Environment: os.Environ(),
		Stdout:      io.Discard,
		Stderr:      io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	state := &executionState{
		plan:               testPlan(),
		workspace:          t.TempDir(),
		temporaryDirectory: t.TempDir(),
		environment:        os.Environ(),
	}
	invocation := &actionInvocation{
		step: Step{Uses: "actions/composite@v1"},
		definition: actionDefinition{Runs: actionRuns{Steps: []compositeStep{{
			Run: "true", Shell: "bash", WorkingDirectory: "../installation-token", ContinueOnError: true,
		}}}},
		inputs: map[string]string{},
	}
	if _, err := executor.runComposite(context.Background(), state, invocation, false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "installation-token") || !strings.Contains(logs.String(), "***") {
		t.Fatalf("warning log = %s", logs.String())
	}
	warning := strings.Index(logs.String(), `"msg":"composite step failed with continue-on-error"`)
	completion := strings.Index(logs.String(), `"msg":"completed composite step"`)
	if warning < 0 || completion < warning {
		t.Fatalf("composite step was not closed after its warning: %s", logs.String())
	}
}

func TestCompositeRunsCancelledStepWithCleanupContext(t *testing.T) {
	workspace := t.TempDir()
	state := &executionState{
		plan:               testPlan(),
		workspace:          workspace,
		temporaryDirectory: t.TempDir(),
		environment:        os.Environ(),
	}
	invocation := &actionInvocation{
		step: Step{Uses: "actions/composite@v1"},
		definition: actionDefinition{Runs: actionRuns{Steps: []compositeStep{
			{Run: "touch composite-default", Shell: "bash"},
			{If: "true", Run: "touch composite-plain-condition", Shell: "bash"},
			{If: "cancelled()", Run: "touch composite-cancelled", Shell: "bash"},
		}}},
		inputs: map[string]string{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := testExecutor(t, io.Discard, io.Discard).runComposite(ctx, state, invocation, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "composite-cancelled")); err != nil {
		t.Fatalf("cancelled composite step did not run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "composite-default")); !os.IsNotExist(err) {
		t.Fatalf("default composite step ran after cancellation: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "composite-plain-condition")); !os.IsNotExist(err) {
		t.Fatalf("plain-condition composite step ran after cancellation: %v", err)
	}
}

func TestCompositeStepsCountTowardEvaluatedContentLimit(t *testing.T) {
	state := &executionState{
		plan:               testPlan(),
		workspace:          t.TempDir(),
		temporaryDirectory: t.TempDir(),
		environment:        os.Environ(),
		resolvedContent:    workflow.MaxJobContentBytes - 7,
	}
	invocation := &actionInvocation{
		step: Step{Uses: "actions/composite@v1"},
		definition: actionDefinition{Runs: actionRuns{Steps: []compositeStep{
			{Run: "true", Shell: "bash"},
			{Run: "true", Shell: "bash"},
		}}},
		inputs: map[string]string{},
	}

	_, err := testExecutor(t, io.Discard, io.Discard).runComposite(context.Background(), state, invocation, false)
	if err == nil || !strings.Contains(err.Error(), "composite step 2: evaluated job configuration exceeds") {
		t.Fatalf("error = %v, want aggregate evaluated job configuration limit", err)
	}
}

func TestMatchesPostConditionDistinguishesCancellationFromFailure(t *testing.T) {
	status := workflowexpression.Status{Cancelled: true}
	if matchesPostCondition("success()", status) {
		t.Fatal("success post condition matched cancellation")
	}
	if matchesPostCondition("failure()", status) {
		t.Fatal("failure post condition matched cancellation")
	}
	if !matchesPostCondition("always()", status) {
		t.Fatal("always post condition did not match cancellation")
	}
}

func waitForFile(t *testing.T, path, timeoutMessage string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(timeoutMessage)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testPlan() *Plan {
	return &Plan{
		Version:      PlanVersion,
		Repository:   Repository{ID: 1, Owner: "acme", Name: "example", ServerURL: "https://github.com", APIURL: "https://api.github.com", ActionCloneBaseURL: "https://github.com"},
		Event:        Event{Name: "push", DeliveryID: "delivery"},
		Revision:     Revision{SHA: strings.Repeat("a", 40), Ref: "refs/heads/main", RefName: "main"},
		WorkflowName: "CI",
		JobID:        "test",
		Steps:        []Step{{Run: "true"}},
	}
}

func testExecutor(t *testing.T, stdout, stderr io.Writer) *Executor {
	t.Helper()
	return testExecutorWithEnvironment(t, os.Environ(), stdout, stderr)
}

func testExecutorWithEnvironment(t *testing.T, environment []string, stdout, stderr io.Writer) *Executor {
	t.Helper()
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		GitHubToken: "installation-token",
		ActionToken: "action-installation-token",
		Environment: environment,
		Stdout:      stdout,
		Stderr:      stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func environmentWithNode24(t *testing.T, nodePath string) []string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Symlink(nodePath, filepath.Join(directory, "node24")); err != nil {
		t.Fatal(err)
	}
	return setEnvironment(os.Environ(), "PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func createActionRepository(t *testing.T, root, owner, name, tag string, files map[string]string) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		path := filepath.Join(source, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	runGit(t, "init", "--quiet", "--initial-branch=main", source)
	runGit(t, "-C", source, "config", "user.name", "Runner Test")
	runGit(t, "-C", source, "config", "user.email", "runner@example.invalid")
	runGit(t, "-C", source, "add", ".")
	runGit(t, "-C", source, "commit", "--quiet", "-m", "fixture")
	runGit(t, "-C", source, "tag", tag)
	bare := filepath.Join(root, owner, name)
	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, "clone", "--quiet", "--bare", source, bare)
}

type authenticatedGitServer struct {
	*httptest.Server
	mutex          sync.Mutex
	requestsByRepo map[string][]string
}

func newAuthenticatedGitServer(t *testing.T, root string, requiredTokens map[string]string) *authenticatedGitServer {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env: []string{
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_PROJECT_ROOT=" + root,
		},
	}
	result := &authenticatedGitServer{requestsByRepo: map[string][]string{}}
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		parts := strings.Split(strings.TrimPrefix(request.URL.Path, "/"), "/")
		if len(parts) < 2 {
			http.NotFound(writer, request)
			return
		}
		repository := parts[0] + "/" + parts[1]
		authorization := request.Header.Get("Authorization")
		result.mutex.Lock()
		result.requestsByRepo[repository] = append(result.requestsByRepo[repository], authorization)
		result.mutex.Unlock()
		if token, required := requiredTokens[repository]; required {
			if authorization != basicAuthorization(token) {
				writer.Header().Set("WWW-Authenticate", `Basic realm="private"`)
				http.Error(writer, "authorization required", http.StatusUnauthorized)
				return
			}
		} else if authorization != "" {
			http.Error(writer, "authorization does not match repository origin", http.StatusBadRequest)
			return
		}
		backend.ServeHTTP(writer, request)
	})
	result.Server = httptest.NewServer(handler)
	t.Cleanup(result.Close)
	return result
}

func (s *authenticatedGitServer) authorizations(repository string) []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]string(nil), s.requestsByRepo[repository]...)
}

func basicAuthorization(token string) string {
	return "basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
}

func runGit(t *testing.T, arguments ...string) string {
	t.Helper()
	output, err := exec.Command("git", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}
