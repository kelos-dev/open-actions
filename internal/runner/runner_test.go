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

func TestLoadPlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.json")
	data := `{"version":1,"repository":{"id":1,"owner":"acme","name":"example","serverURL":"https://github.com","apiURL":"https://api.github.com","actionCloneBaseURL":"https://github.com"},"event":{"name":"push","deliveryID":"delivery"},"revision":{"sha":"abc","ref":"refs/heads/main","refName":"main"},"workflowName":"CI","jobID":"build","steps":[{"run":"true"}]}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := LoadPlan(path)
	if err != nil {
		t.Fatalf("load plan: %v", err)
	}
	if plan.JobID != "build" {
		t.Errorf("job ID = %q", plan.JobID)
	}
}

func TestLoadPlanRejectsIncompatibleSchemas(t *testing.T) {
	for name, data := range map[string]string{
		"unversioned":         `{"repository":{}}`,
		"unsupported version": `{"version":2,"repository":{}}`,
		"unknown field":       `{"version":1,"futureField":true}`,
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
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(workspace, "started")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("JavaScript action did not start")
				}
				time.Sleep(10 * time.Millisecond)
			}
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
    - id: ignored-failure
      run: exit 1
      shell: bash
      continue-on-error: true
    - id: nested
      uses: actions/nested@v1
      with:
        message: ${{ inputs.message }}
    - name: Export nested output
      run: |
        test -f "${{ github.action_path }}/action.yml"
        test "$EXPECTED" = "composite value"
        test "$OUTER" = "workflow action env"
        test "$EXPECTED_OUTER" = "workflow action env"
        printf 'COMPOSITE_VALUE=%s\n' "${{ steps.nested.outputs.value }}" >> "$GITHUB_ENV"
      shell: bash
      env:
        EXPECTED: ${{ inputs.message }}
        EXPECTED_OUTER: ${{ env.OUTER }}
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
	inputs, err := actionInputs(map[string]actionInput{
		"repository": {Default: "${{ github.repository }}"},
		"token":      {Default: "${{ github.token }}"},
	}, nil, plan, "installation-token")
	if err != nil {
		t.Fatal(err)
	}
	if inputs["repository"] != "acme/example" || inputs["token"] != "installation-token" {
		t.Errorf("inputs = %#v", inputs)
	}
}

func TestActionInputsMergeCaseInsensitively(t *testing.T) {
	inputs, err := actionInputs(
		map[string]actionInput{"message": {Default: "default"}},
		map[string]string{"MESSAGE": "supplied"},
		testPlan(),
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
			if _, err := actionInputs(tt.definitions, tt.supplied, testPlan(), "installation-token"); err == nil {
				t.Fatal("actionInputs() accepted case-insensitive duplicate names")
			}
		})
	}
}

func TestActionInputsRejectUnsupportedDefaultExpressions(t *testing.T) {
	for _, value := range []string{"${{ github.sha }}", "prefix-${{ github.repository }}"} {
		_, err := actionInputs(map[string]actionInput{"value": {Default: value}}, nil, testPlan(), "installation-token")
		if err == nil || !strings.Contains(err.Error(), "unsupported expression") {
			t.Fatalf("actionInputs() error for %q = %v", value, err)
		}
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
	if _, err := executor.runComposite(context.Background(), state, invocation, 1); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "installation-token") || !strings.Contains(logs.String(), "***") {
		t.Fatalf("warning log = %s", logs.String())
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
