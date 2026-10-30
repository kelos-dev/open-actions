package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kelos-dev/open-actions/internal/actionref"
	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
)

func TestActionLifecycleConditions(t *testing.T) {
	state := &executionState{
		plan: testPlan(), environment: []string{"RUNNER_OS=Linux"},
		contextEnvironment: map[string]string{"ENABLED": "false"},
		stepOutputs:        map[string]map[string]any{"build": {"conclusion": "failure"}},
	}
	invocation := &actionInvocation{step: Step{Env: map[string]string{"ENABLED": "true"}}, reference: actionref.Reference{Owner: "actions", Repository: "example", Ref: "v1"}}
	for _, test := range []struct {
		name, condition      string
		status               workflowexpression.Status
		pre, want, wantError bool
	}{
		{name: "default after failure", status: workflowexpression.Status{Failure: true}, want: true},
		{name: "default after cancellation", status: workflowexpression.Status{Cancelled: true}, want: true},
		{name: "success", condition: "success()", status: workflowexpression.Status{Success: true}, want: true},
		{name: "success excludes cancellation", condition: "success()", status: workflowexpression.Status{Cancelled: true}},
		{name: "failure excludes cancellation", condition: "failure()", status: workflowexpression.Status{Cancelled: true}},
		{name: "always includes cancellation", condition: "always()", status: workflowexpression.Status{Cancelled: true}, want: true},
		{name: "cancelled", condition: "cancelled() && !failure()", status: workflowexpression.Status{Cancelled: true}, want: true},
		{name: "pre uses job-start status", condition: "success() && job.status == 'success' && !failure()", status: workflowStepStatus(false, false), pre: true, want: true},
		{name: "step environment overrides job environment", condition: "env.ENABLED == 'true' && runner.os == 'Linux'", status: workflowexpression.Status{Success: true}, pre: true, want: true},
		{name: "explicit condition has no implicit success", condition: "env.ENABLED == 'true'", status: workflowexpression.Status{Failure: true}, want: true},
		{name: "delimited expression", condition: "${{ always() && github.action_repository == 'actions/example' }}", status: workflowexpression.Status{Failure: true}, want: true},
		{name: "post reads workflow step result", condition: "failure() && steps.build.conclusion == 'failure'", status: workflowexpression.Status{Failure: true}, want: true},
		{name: "pre rejects steps", condition: "steps.build.conclusion == 'failure'", pre: true, wantError: true},
		{name: "reject secrets", condition: "secrets.TOKEN != ''", wantError: true},
		{name: "runtime error", condition: "fromJSON('invalid').enabled", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			availability := actionPostConditionAvailability
			if test.pre {
				availability = actionPreConditionAvailability
			}
			got, err := actionLifecycleCondition(test.condition, invocation, state, test.status, availability)
			if (err != nil) != test.wantError || got != test.want {
				t.Fatalf("condition = %t, %v; want %t, error %t", got, err, test.want, test.wantError)
			}
		})
	}
}

func TestExecuteRustCacheLifecycleCondition(t *testing.T) {
	for _, program := range []string{"git", "node"} {
		if _, err := exec.LookPath(program); err != nil {
			t.Skipf("%s is not installed", program)
		}
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "rust-cache", "v2", map[string]string{
		"action.yml": `name: Rust cache lifecycle fixture
inputs:
  cache-on-failure:
    default: 'false'
runs:
  using: node20
  pre: pre.js
  pre-if: runner.os == 'Linux' && env.RUN_PRE == 'true'
  main: main.js
  post: post.js
  post-if: "success() || env.CACHE_ON_FAILURE == 'true'"
`,
		"pre.js": `require('fs').writeFileSync('pre-ran', 'pre');`,
		"main.js": `const fs = require('fs');
fs.writeFileSync('main-ran', 'main');
fs.appendFileSync(process.env.GITHUB_ENV, 'CACHE_ON_FAILURE=' + process.env['INPUT_CACHE-ON-FAILURE'] + '\n');`,
		"post.js": `require('fs').writeFileSync('post-ran', 'post');`,
	})
	for _, test := range []struct {
		name                            string
		fail, cacheOnFailure, pre, post bool
	}{
		{name: "successful job saves", pre: true, post: true},
		{name: "successful job with skipped pre saves", post: true},
		{name: "failed job skips save", fail: true},
		{name: "failed job saves when enabled", fail: true, cacheOnFailure: true, pre: true, post: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := testPlan()
			plan.Repository.ActionCloneBaseURL = "file://" + repositories
			plan.Env = map[string]string{"RUN_PRE": "false", "CACHE_ON_FAILURE": "false"}
			script := "true"
			if test.fail {
				script = "exit 1"
			}
			plan.Steps = []Step{
				{Uses: "actions/rust-cache@v2", With: map[string]string{"cache-on-failure": fmt.Sprint(test.cacheOnFailure)}, Env: map[string]string{"RUN_PRE": fmt.Sprint(test.pre)}},
				{Run: script},
			}
			workspace := t.TempDir()
			result, err := testExecutor(t, io.Discard, io.Discard).ExecuteResult(context.Background(), plan, workspace)
			if (err != nil) != test.fail {
				t.Fatalf("execute error = %v, want error %t", err, test.fail)
			}
			wantConclusion := ResultConclusionSuccess
			if test.fail {
				wantConclusion = ResultConclusionFailure
			}
			if result.Conclusion != wantConclusion {
				t.Fatalf("conclusion = %q, want %q", result.Conclusion, wantConclusion)
			}
			for name, want := range map[string]bool{"pre-ran": test.pre, "main-ran": true, "post-ran": test.post} {
				data, err := os.ReadFile(filepath.Join(workspace, name))
				if want {
					if err != nil || string(data) != strings.TrimSuffix(name, "-ran") {
						t.Fatalf("%s contents = %q, error = %v", name, data, err)
					}
				} else if !os.IsNotExist(err) {
					t.Fatalf("%s ran despite its condition: %v", name, err)
				}
			}
		})
	}
}

func TestExecuteRejectsInvalidLifecycleConditionsBeforeSteps(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	for _, test := range []struct{ name, field, condition string }{
		{"invalid pre syntax", "pre-if", "success() ||"},
		{"pre step context", "pre-if", "steps.previous.conclusion == 'success'"},
		{"invalid post syntax", "post-if", "success() &&"},
		{"unknown post function", "post-if", "unknown()"},
		{"secret post context", "post-if", "secrets.TOKEN != ''"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repositories := t.TempDir()
			createActionRepository(t, repositories, "actions", "invalid", "v1", map[string]string{
				"action.yml": fmt.Sprintf("name: Invalid lifecycle fixture\nruns:\n  using: node20\n  main: main.js\n  %s: %q\n", test.field, test.condition),
				"main.js":    "",
			})
			plan := testPlan()
			plan.Repository.ActionCloneBaseURL = "file://" + repositories
			plan.Steps = []Step{{Run: "touch workflow-ran"}, {Uses: "actions/invalid@v1"}}
			workspace := t.TempDir()
			err := testExecutor(t, io.Discard, io.Discard).Execute(context.Background(), plan, workspace)
			if err == nil || !strings.Contains(err.Error(), "action actions/invalid@v1 runs."+test.field+":") {
				t.Fatalf("preparation error = %v", err)
			}
			if _, err := os.Stat(filepath.Join(workspace, "workflow-ran")); !os.IsNotExist(err) {
				t.Fatalf("workflow steps ran before lifecycle validation: %v", err)
			}
		})
	}
}

func TestExecutePreLifecycleConditionUsesJobStartStatus(t *testing.T) {
	for _, program := range []string{"git", "node"} {
		if _, err := exec.LookPath(program); err != nil {
			t.Skipf("%s is not installed", program)
		}
	}
	for _, test := range []struct {
		condition string
		wantPre   bool
	}{
		{condition: "success()", wantPre: true},
		{condition: "failure()"},
		{condition: "cancelled()"},
		{condition: "success() && job.status == 'success' && !failure()", wantPre: true},
	} {
		t.Run(test.condition, func(t *testing.T) {
			repositories := t.TempDir()
			createActionRepository(t, repositories, "actions", "setup", "v1", map[string]string{
				"action.yml": fmt.Sprintf("name: Setup fixture\nruns:\n  using: node20\n  pre: pre.js\n  pre-if: %q\n  main: main.js\n", test.condition),
				"pre.js":     `require('fs').writeFileSync('pre-ran', 'pre');`,
				"main.js":    `require('fs').writeFileSync('main-ran', 'main');`,
			})
			plan := testPlan()
			plan.Repository.ActionCloneBaseURL = "file://" + repositories
			plan.Steps = []Step{{Run: "exit 1"}, {Uses: "actions/setup@v1", If: "always()"}}
			workspace := t.TempDir()
			result, err := testExecutor(t, io.Discard, io.Discard).ExecuteResult(context.Background(), plan, workspace)
			if err == nil || result.Conclusion != ResultConclusionFailure {
				t.Fatalf("earlier step failure was not preserved: result=%#v error=%v", result, err)
			}
			data, err := os.ReadFile(filepath.Join(workspace, "main-ran"))
			if err != nil || string(data) != "main" {
				t.Fatalf("main hook did not run: contents=%q error=%v", data, err)
			}
			data, err = os.ReadFile(filepath.Join(workspace, "pre-ran"))
			if test.wantPre {
				if err != nil || string(data) != "pre" {
					t.Fatalf("pre hook did not use job-start success: contents=%q error=%v", data, err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("pre hook ran with mid-job status: contents=%q error=%v", data, err)
			}
		})
	}
}

func TestPostLifecycleConditionErrorFailsJobAndContinuesCleanup(t *testing.T) {
	directory, workspace := t.TempDir(), t.TempDir()
	for name, script := range map[string]string{"cleanup.sh": "printf cleanup > cleanup-ran\n", "broken.sh": "touch broken-ran\n"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(script), 0600); err != nil {
			t.Fatal(err)
		}
	}
	state := &executionState{
		plan: testPlan(), workspace: workspace, temporaryDirectory: t.TempDir(), environment: os.Environ(),
		contextEnvironment: map[string]string{"INVALID": "{"}, jobStatus: workflowexpression.Status{Success: true},
		posts: []*actionInvocation{
			{step: Step{Uses: "actions/cleanup@v1"}, directory: directory, executable: "/bin/sh", definition: actionDefinition{Runs: actionRuns{Post: "cleanup.sh", PostIf: "failure() && job.status == 'failure'"}}},
			{step: Step{Uses: "actions/broken@v1"}, directory: directory, executable: "/bin/sh", definition: actionDefinition{Runs: actionRuns{Post: "broken.sh", PostIf: "fromJSON(env.INVALID).enabled"}}},
		},
	}
	err := testExecutor(t, io.Discard, io.Discard).runPostActions(context.Background(), state, workflowexpression.Status{Success: true})
	if err == nil || !strings.Contains(err.Error(), "post action actions/broken@v1: evaluate post-if:") {
		t.Fatalf("post condition error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "broken-ran")); !os.IsNotExist(err) {
		t.Fatalf("hook with failed condition evaluation ran: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(workspace, "cleanup-ran"))
	if err != nil || string(data) != "cleanup" {
		t.Fatalf("remaining cleanup contents = %q, error = %v", data, err)
	}
}
