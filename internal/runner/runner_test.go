package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kelos-dev/open-actions/internal/actionref"
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
	"github.com/kelos-dev/open-actions/internal/workflow"
)

func TestExecuteRunSteps(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Env = map[string]string{"JOB_ENV": "job"}
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
			Run:  `printf '%s/%s/%s/%s/%s' "$JOB_ENV" "$STEP_ENV" "$GITHUB_REF_NAME" "$EXPORTED" "${PATH%%:*}" > result`,
			Env:  map[string]string{"STEP_ENV": "step"},
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
	if string(result) != "job/step/main/value/"+binDirectory {
		t.Errorf("result = %q", result)
	}
}

func TestExecuteEvaluatesWorkflowExpressions(t *testing.T) {
	workspace := t.TempDir()
	plan := testPlan()
	plan.Env = map[string]string{"BRANCH": "${{ github.ref_name }}", "JOB_TOKEN": "${{ github.token }}"}
	plan.Steps = []Step{
		{
			Name: "skipped",
			If:   "github.ref_name != 'main'",
			Run:  "touch skipped ${{ secrets.TOKEN }}",
			Env:  map[string]string{"TOKEN": "${{ secrets.TOKEN }}"},
		},
		{
			Name: "write ${{ github.ref_name }} result",
			If:   "env.TARGET == 'main'",
			Env: map[string]string{
				"REPOSITORY": "${{ github.repository }}",
				"STEP_TOKEN": "${{ github.token }}",
				"TARGET":     "main",
			},
			Run: "test \"$JOB_TOKEN\" = installation-token && test \"$STEP_TOKEN\" = installation-token && printf '%s/%s/${{ github.sha }}/${{ env.TARGET }}' \"$BRANCH\" \"$REPOSITORY\" > result",
		},
	}
	executor := testExecutor(t, io.Discard, io.Discard)
	if err := executor.Execute(context.Background(), plan, workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "skipped")); !os.IsNotExist(err) {
		t.Fatalf("skipped step created a file: %v", err)
	}
	result, err := os.ReadFile(filepath.Join(workspace, "result"))
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "main/acme/example/"+strings.Repeat("a", 40)+"/main" {
		t.Fatalf("result = %q", result)
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

func TestExecuteRejectsUnavailableExpressionContext(t *testing.T) {
	plan := testPlan()
	plan.Env = map[string]string{"TOKEN": "${{ secrets.TOKEN }}"}
	err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), `context "secrets" is unavailable`) {
		t.Fatalf("error = %v, want unavailable secrets context", err)
	}
}

func TestLoadPlanSupportsVersionsOneAndTwo(t *testing.T) {
	for _, version := range []int{minimumPlanVersion, PlanVersion} {
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

func TestLoadPlanRejectsIncompatibleSchemas(t *testing.T) {
	for name, data := range map[string]string{
		"unversioned":         `{"repository":{}}`,
		"unsupported version": `{"version":3,"repository":{}}`,
		"unknown field":       `{"version":2,"futureField":true}`,
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

func TestNewExecutorRequiresGitHubToken(t *testing.T) {
	_, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Environment: os.Environ(),
		Stdout:      io.Discard,
		Stderr:      io.Discard,
	})
	if err == nil {
		t.Fatal("NewExecutor() accepted an empty GitHub token")
	}
}

func TestActionCloneTokenIsLimitedToWorkflowRepository(t *testing.T) {
	plan := testPlan()
	sameRepository := actionref.Reference{Owner: "ACME", Repository: "Example"}
	otherRepository := actionref.Reference{Owner: "actions", Repository: "checkout"}
	if got := actionCloneToken(sameRepository, plan, "installation-token"); got != "installation-token" {
		t.Fatalf("same-repository clone token = %q", got)
	}
	if got := actionCloneToken(otherRepository, plan, "installation-token"); got != "" {
		t.Fatalf("cross-repository clone token = %q", got)
	}

	environment := actionDownloadEnvironment([]string{"PATH=/usr/bin"}, "https://github.example/acme/example", "installation-token")
	for _, name := range []string{"GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0", "GIT_CONFIG_VALUE_0"} {
		if environmentValue(environment, name) == "" {
			t.Fatalf("%s was not configured", name)
		}
	}
	for _, entry := range environment {
		if strings.Contains(entry, "installation-token") {
			t.Fatalf("download environment contains the raw token: %q", entry)
		}
	}
	if got := actionDownloadEnvironment([]string{"PATH=/usr/bin"}, "https://github.example/actions/checkout", ""); len(got) != 1 {
		t.Fatalf("unauthenticated download environment = %#v", got)
	}
}

func TestGitHubEventDocumentContainsNormalizedContext(t *testing.T) {
	for _, event := range []Event{
		{Name: "push", DeliveryID: "push-delivery"},
		{Name: "pull_request", Action: "synchronize", DeliveryID: "pr-delivery"},
		{Name: "merge_group", Action: "checks_requested", DeliveryID: "merge-delivery"},
	} {
		plan := testPlan()
		plan.Event = event
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
		case "pull_request":
			if document["pull_request"] == nil {
				t.Fatalf("pull request document = %#v", document)
			}
		case "merge_group":
			if document["merge_group"] == nil {
				t.Fatalf("merge group document = %#v", document)
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
if (process.env.PRE_VALUE !== 'ready' || !process.env.PATH.startsWith('/external/bin:')) {
  throw new Error('pre command files were not applied');
}
fs.appendFileSync(process.env.GITHUB_ENV, 'ACTION_VALUE<<EOF\n' + process.env['INPUT_MESSAGE'] + '\nEOF\n');
fs.appendFileSync(process.env.GITHUB_OUTPUT, 'value=' + process.env['INPUT_MESSAGE'] + '\n');
fs.appendFileSync(process.env.GITHUB_STATE, 'main_marker=saved\n');
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
			plan.Steps = []Step{
				{Run: `printf '%s\n' "$RUNTIME_OVERRIDE" >> "$GITHUB_PATH"`, Env: map[string]string{"RUNTIME_OVERRIDE": overrideDirectory}},
				{Uses: "actions/example@v1", With: map[string]string{"message": "external action ran"}},
				{Run: `test "$ACTION_VALUE" = "external action ran" && case "$PATH" in /external/bin:*) ;; *) exit 1 ;; esac`},
			}
			var output bytes.Buffer
			executor := testExecutorWithEnvironment(t, environment, &output, &output)
			if err := executor.Execute(context.Background(), plan, t.TempDir()); err != nil {
				t.Fatalf("execute external action: %v: %s", err, output.String())
			}
			if !strings.Contains(output.String(), "external pre ran") || !strings.Contains(output.String(), "external post ran") {
				t.Errorf("lifecycle output was not recorded: %s", output.String())
			}
			if strings.Contains(output.String(), "runtime-secret") || !strings.Contains(output.String(), "***") {
				t.Errorf("action output was not masked: %s", output.String())
			}
		})
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
        test "$TOKEN" = "installation-token"
        test "${{ env.LOCAL }}" = "composite-local"
        printf 'COMPOSITE_VALUE=%s\n' "${{ steps.nested.outputs.value }}" >> "$GITHUB_ENV"
      shell: bash
      env:
        EXPECTED: ${{ inputs.message }}
        EXPECTED_OUTER: ${{ env.OUTER }}
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
	plan.Steps = []Step{
		{Uses: "actions/parent@v1", With: map[string]string{"message": "composite value"}, Env: map[string]string{"OUTER": "workflow action env"}},
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
	resolver := newActionResolver("https://github.example", directory, []string{"PATH=/usr/bin"}, func(_ context.Context, command string, arguments []string, _ string, _ []string) error {
		if command == "git" && len(arguments) >= 3 && arguments[0] == "init" {
			downloadDirectories = append(downloadDirectories, arguments[2])
			if err := os.MkdirAll(arguments[2], 0o755); err != nil {
				return err
			}
		}
		return errors.New("download failed")
	})
	step := Step{Uses: "actions/example@v1"}
	for range 2 {
		if _, err := resolver.prepare(context.Background(), step, testPlan(), "token"); err == nil {
			t.Fatal("prepare() succeeded after a failed action download")
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

func TestOutputMaskerHandlesAddMaskCommand(t *testing.T) {
	var output bytes.Buffer
	masker := newOutputMasker("initial-secret")
	writer := masker.writer(&output)
	_, err := writer.Write([]byte("initial-secret\n::add-mask::dynamic%25secret\ndynamic%secret\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "***\n***\n" {
		t.Errorf("output = %q", output.String())
	}
}

func TestOutputMaskerMasksEachLineOfMultilineAddMask(t *testing.T) {
	var output bytes.Buffer
	writer := newOutputMasker().writer(&output)
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

func TestOutputMaskerPreservesPartialCommandPrefixes(t *testing.T) {
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
	commandWriter := executor.masker.writer(io.Discard)
	if _, err := commandWriter.Write([]byte("::add-mask::dynamic-secret\n")); err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	plan.Steps = []Step{{Run: "true", WorkingDirectory: "../dynamic-secret"}}
	err := executor.Execute(context.Background(), plan, t.TempDir())
	if err == nil || strings.Contains(err.Error(), "dynamic-secret") || !strings.Contains(err.Error(), "***") {
		t.Fatalf("fatal error = %v", err)
	}
}

func TestExecutorMasksContinueOnErrorWarnings(t *testing.T) {
	var logs bytes.Buffer
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewJSONHandler(&logs, nil)),
		GitHubToken: "installation-token",
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
		compositeStack:     map[string]bool{},
	}
	invocation := &actionInvocation{
		step: Step{Uses: "actions/composite@v1"},
		definition: actionDefinition{Runs: actionRuns{Steps: []compositeStep{{
			Run: "true", Shell: "bash", WorkingDirectory: "../installation-token", ContinueOnError: true,
		}}}},
		inputs: map[string]string{},
	}
	if _, err := executor.runComposite(context.Background(), state, invocation, 1, false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "installation-token") || !strings.Contains(logs.String(), "***") {
		t.Fatalf("warning log = %s", logs.String())
	}
}

func TestCompositeRunsCancelledStepWithCleanupContext(t *testing.T) {
	workspace := t.TempDir()
	state := &executionState{
		plan:               testPlan(),
		workspace:          workspace,
		temporaryDirectory: t.TempDir(),
		environment:        os.Environ(),
		compositeStack:     map[string]bool{},
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
	if _, err := testExecutor(t, io.Discard, io.Discard).runComposite(ctx, state, invocation, 1, false); err != nil {
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
		compositeStack:     map[string]bool{},
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

	_, err := testExecutor(t, io.Discard, io.Discard).runComposite(context.Background(), state, invocation, 1, false)
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

func runGit(t *testing.T, arguments ...string) string {
	t.Helper()
	output, err := exec.Command("git", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}
