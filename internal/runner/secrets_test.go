package runner

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestExecutePreservesUnusedProjectSecretValues(t *testing.T) {
	for name, step := range map[string]Step{
		"unreferenced":         {Run: "true"},
		"literal context name": {Run: "test '${{ 'secrets.REGISTRY_USER' }}' = secrets.REGISTRY_USER"},
	} {
		t.Run(name, func(t *testing.T) {
			plan := testPlan()
			plan.Env = map[string]string{"TOKEN": "${{ secrets.GITHUB_TOKEN }}"}
			plan.Steps = []Step{step, {
				ID:  "publish",
				Run: `printf 'https://registry-user.example/report\n'; printf 'registry-user\n' >&2; printf 'url=https://registry-user.example/report\n' >> "$GITHUB_OUTPUT"`,
			}}
			plan.Outputs = map[string]string{"url": "${{ steps.publish.outputs.url }}"}
			var output, stderr bytes.Buffer
			executor := projectSecretExecutor(t, &output, &stderr)
			result, err := executor.ExecuteResult(context.Background(), plan, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if got, want := output.String(), "https://registry-user.example/report\n"; got != want {
				t.Fatalf("output = %q, want %q", got, want)
			}
			if got := stderr.String(); got != "registry-user\n" {
				t.Fatalf("stderr = %q", got)
			}
			if result.Outputs["url"] != "https://registry-user.example/report" {
				t.Fatalf("job outputs = %#v", result.Outputs)
			}
		})
	}
}

func TestExecuteMasksReferencedProjectSecrets(t *testing.T) {
	for name, test := range map[string]struct {
		jobEnv     map[string]string
		step       Step
		maskUnused bool
	}{
		"job environment": {
			jobEnv: map[string]string{"TOKEN": "${{ secrets.REGISTRY_USER }}"},
			step:   Step{Run: `test "$TOKEN" = registry-user; printf '%s\n' "$TOKEN"`},
		},
		"step environment": {
			step: Step{Env: map[string]string{"TOKEN": "${{ secrets.registry_user }}"}, Run: `test "$TOKEN" = registry-user; printf '%s\n' "$TOKEN"`},
		},
		"script interpolation": {
			step: Step{Run: `test '${{ secrets.REGISTRY_USER }}' = registry-user; printf '%s\n' '${{ secrets.REGISTRY_USER }}'`},
		},
		"dynamic lookup": {
			maskUnused: true,
			jobEnv:     map[string]string{"SECRET_NAME": "registry_user"},
			step:       Step{Env: map[string]string{"TOKEN": "${{ secrets[env.SECRET_NAME] }}"}, Run: `test "$TOKEN" = registry-user; printf '%s\n' "$TOKEN"`},
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan := testPlan()
			plan.Env = test.jobEnv
			plan.Steps = []Step{test.step, {
				ID:  "publish",
				Run: `printf 'https://registry-user.example/report\nunrelated-value\n'; printf 'registry-user\n' >&2; printf 'url=https://registry-user.example/report\npublic=ready\n' >> "$GITHUB_OUTPUT"`,
			}}
			plan.Outputs = map[string]string{"url": "${{ steps.publish.outputs.url }}", "public": "${{ steps.publish.outputs.public }}"}
			var output, stderr bytes.Buffer
			executor := projectSecretExecutor(t, &output, &stderr)
			result, err := executor.ExecuteResult(context.Background(), plan, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			unused := "unrelated-value"
			if test.maskUnused {
				unused = "***"
			}
			if got, want := output.String(), "***\nhttps://***.example/report\n"+unused+"\n"; got != want {
				t.Fatalf("output = %q, want %q", got, want)
			}
			if got := stderr.String(); got != "***\n" {
				t.Fatalf("stderr = %q", got)
			}
			if !maps.Equal(result.Outputs, map[string]string{"public": "ready"}) {
				t.Fatalf("job outputs = %#v", result.Outputs)
			}
		})
	}
}

// GitHub initializes referenced secrets before job execution, including references
// in skipped steps: https://docs.github.com/en/actions/concepts/security/compromised-runners#accessing-secrets
// https://github.com/actions/runner/blob/main/src/Runner.Worker/Worker.cs#L87
func TestExecuteMasksSecretsBeforeReferencingStep(t *testing.T) {
	for name, step := range map[string]Step{
		"later step":    {Run: "test '${{ secrets.REGISTRY_USER }}' = registry-user"},
		"skipped step":  {If: "false", Env: map[string]string{"TOKEN": "${{ secrets.REGISTRY_USER }}"}, Run: "exit 1"},
		"short circuit": {Run: "test '${{ false && secrets.REGISTRY_USER }}' = false"},
	} {
		t.Run(name, func(t *testing.T) {
			plan := testPlan()
			plan.Steps = []Step{{ID: "publish", Run: `printf 'registry-user\n'; printf 'url=https://registry-user.example/report\n' >> "$GITHUB_OUTPUT"`}, step}
			plan.Outputs = map[string]string{"url": "${{ steps.publish.outputs.url }}", "public": "ready"}
			var output bytes.Buffer
			executor := projectSecretExecutor(t, &output, io.Discard)
			result, err := executor.ExecuteResult(context.Background(), plan, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if got := output.String(); got != "***\n" {
				t.Fatalf("output = %q", got)
			}
			if !maps.Equal(result.Outputs, map[string]string{"public": "ready"}) {
				t.Fatalf("job outputs = %#v", result.Outputs)
			}
		})
	}
}

func TestRegisterJobSecretMasksIncludesWholeContext(t *testing.T) {
	for _, input := range []string{"${{ toJSON(secrets) }}", "${{ join(secrets.*, ',') }}", "${{ secrets }}", "${{ secrets[env.NAME] }}"} {
		t.Run(input, func(t *testing.T) {
			executor := projectSecretExecutor(t, io.Discard, io.Discard)
			executor.secrets["actions_step_debug"] = "true"
			plan := testPlan()
			plan.Env = map[string]string{"VALUE": input}
			if err := executor.registerJobSecretMasks(plan); err != nil {
				t.Fatal(err)
			}
			if got := executor.masker.mask("registry-user unrelated-value installation-token true"); got != "*** *** *** true" {
				t.Fatalf("masked values = %q", got)
			}
		})
	}
}

func TestRegisterJobSecretMasksScansExpressionFields(t *testing.T) {
	for name, configure := range map[string]func(*Plan){
		"job env":           func(p *Plan) { p.Env = map[string]string{"VALUE": "${{ secrets.REGISTRY_USER }}"} },
		"job output":        func(p *Plan) { p.Outputs = map[string]string{"value": "${{ secrets.REGISTRY_USER }}"} },
		"step env":          func(p *Plan) { p.Steps[0].Env = map[string]string{"VALUE": "${{ secrets.REGISTRY_USER }}"} },
		"step input":        func(p *Plan) { p.Steps[0].With = map[string]string{"value": "${{ secrets.REGISTRY_USER }}"} },
		"step name":         func(p *Plan) { p.Steps[0].Name = "${{ secrets.REGISTRY_USER }}" },
		"step script":       func(p *Plan) { p.Steps[0].Run = "${{ secrets.REGISTRY_USER }}" },
		"working directory": func(p *Plan) { p.Steps[0].WorkingDirectory = "${{ secrets.REGISTRY_USER }}" },
		"continue on error": func(p *Plan) { p.Steps[0].ContinueOnError = "${{ secrets.REGISTRY_USER != '' }}" },
	} {
		t.Run(name, func(t *testing.T) {
			plan := testPlan()
			plan.Steps = []Step{{Run: "true"}}
			configure(plan)
			executor := projectSecretExecutor(t, io.Discard, io.Discard)
			if err := executor.registerJobSecretMasks(plan); err != nil {
				t.Fatal(err)
			}
			if got := executor.masker.mask("registry-user unrelated-value"); got != "*** unrelated-value" {
				t.Fatalf("masked values = %q", got)
			}
		})
	}
}

func TestExecuteMasksProjectSecretPassedToAction(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repositories := t.TempDir()
	createActionRepository(t, repositories, "actions", "secret-input", "v1", map[string]string{
		"action.yml": `name: Secret input fixture
inputs:
  token:
    required: true
runs:
  using: composite
  steps:
    - run: |
        test "$TOKEN" = registry-user
        printf '%s\n' "$TOKEN"
      shell: bash
      env:
        TOKEN: ${{ inputs.token }}
`,
	})
	plan := testPlan()
	plan.Repository.ActionCloneBaseURL = "file://" + repositories
	plan.Steps = []Step{{Uses: "actions/secret-input@v1", With: map[string]string{"token": "${{ secrets.REGISTRY_USER }}"}}}
	var output bytes.Buffer
	executor := projectSecretExecutor(t, &output, io.Discard)
	if err := executor.Execute(context.Background(), plan, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); strings.Contains(got, "registry-user") || !strings.Contains(got, "***\n") {
		t.Fatalf("action output = %q", got)
	}
}

func TestExecuteMasksSecretsReferencedOnlyByJobOutputs(t *testing.T) {
	plan := testPlan()
	plan.Steps = []Step{{ID: "publish", Run: `printf 'registry-user\n'; printf 'url=https://registry-user.example/report\n' >> "$GITHUB_OUTPUT"`}}
	plan.Outputs = map[string]string{
		"url":    "${{ steps.publish.outputs.url }}",
		"secret": "${{ secrets.REGISTRY_USER }}",
		"public": "ready",
	}
	var output bytes.Buffer
	executor := projectSecretExecutor(t, &output, io.Discard)
	result, err := executor.ExecuteResult(context.Background(), plan, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(result.Outputs, map[string]string{"public": "ready"}) {
		t.Fatalf("job outputs = %#v", result.Outputs)
	}
	if got := output.String(); got != "***\n" {
		t.Fatalf("output = %q", got)
	}
}

func TestReferencedSecretIsMaskedInDiagnostic(t *testing.T) {
	plan := testPlan()
	plan.Steps = []Step{{Env: map[string]string{"VALUE": "${{ format(secrets.REGISTRY_USER, 'value') }}"}, Run: "true"}}
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		RunnerName:  "runner-1",
		GitHubToken: "installation-token",
		Secrets:     map[string]string{"REGISTRY_USER": "invalid-{format"},
		Environment: os.Environ(),
		Stdout:      io.Discard,
		Stderr:      io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.ExecuteResult(context.Background(), plan, t.TempDir())
	if err == nil || strings.Contains(err.Error(), "invalid-{format") {
		t.Fatalf("execution error = %v", err)
	}
	if got := executor.masker.mask("invalid-{format"); got != "***" {
		t.Fatalf("masked diagnostic value = %q", got)
	}
}

func projectSecretExecutor(t *testing.T, stdout, stderr io.Writer) *Executor {
	t.Helper()
	executor, err := NewExecutor(ExecutorConfig{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		RunnerName:  "runner-1",
		GitHubToken: "installation-token",
		Secrets:     map[string]string{"REGISTRY_USER": "registry-user", "UNUSED": "unrelated-value"},
		Environment: os.Environ(),
		Stdout:      stdout,
		Stderr:      stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}
