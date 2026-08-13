package expression

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestTemplateEvaluationPreservesTypesAndLiteralText(t *testing.T) {
	context := Context{
		Availability: NewAvailability("matrix"),
		Values: map[string]any{
			"matrix": map[string]any{"enabled": true, "arch": "arm64"},
		},
	}
	tests := []struct {
		name  string
		input string
		want  any
	}{
		{name: "boolean", input: "${{ matrix.enabled }}", want: true},
		{name: "interpolation", input: "linux/${{ matrix.arch }}", want: "linux/arm64"},
		{name: "multiple expressions", input: "${{ matrix.arch }}-${{ matrix.enabled }}", want: "arm64-true"},
		{name: "format braces", input: "${{ format('{{{0}}}', matrix.arch) }}", want: "{arm64}"},
		{name: "literal", input: "plain text", want: "plain text"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := evaluateForTest(t, tt.input, context)
			if fmt.Sprint(result.Value) != fmt.Sprint(tt.want) {
				t.Fatalf("value = %#v, want %#v", result.Value, tt.want)
			}
			if _, ok := tt.want.(bool); ok {
				if _, ok := result.Value.(bool); !ok {
					t.Fatalf("value type = %T, want bool", result.Value)
				}
			}
		})
	}
}

func TestOperatorsUseGitHubCoercionAndShortCircuiting(t *testing.T) {
	availability := NewAvailability("github")
	context := Context{Availability: availability, Values: map[string]any{"github": map[string]any{"ref_name": "main"}}}
	tests := []struct {
		expression string
		want       any
	}{
		{expression: "'' == false", want: true},
		{expression: "null == 0", want: true},
		{expression: "'MAIN' == 'main'", want: true},
		{expression: "'2' > 1", want: true},
		{expression: "0xff == 255", want: true},
		{expression: "-0Xf == -15", want: true},
		{expression: "'not-a-number' > 1", want: false},
		{expression: "!null", want: true},
		{expression: "0 || 'fallback'", want: "fallback"},
		{expression: "true && 'selected' || 'fallback'", want: "selected"},
		{expression: "github.ref_name || format('{2}', 'unused')", want: "main"},
		{expression: "false && format('{2}', 'unused')", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			program, err := ParseCondition(tt.expression)
			if err != nil {
				t.Fatal(err)
			}
			result, err := program.Evaluate(context)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(result.Value) != fmt.Sprint(tt.want) {
				t.Fatalf("value = %#v, want %#v", result.Value, tt.want)
			}
		})
	}
}

func TestDeferredObjectOnlyResolvesReadProperties(t *testing.T) {
	resolved := 0
	context := Context{
		Availability: NewAvailability("env"),
		Values: map[string]any{
			"env": DeferredObject(func(name string) (any, bool, error) {
				resolved++
				if strings.EqualFold(name, "target") {
					return "main", true, nil
				}
				return nil, false, nil
			}),
		},
	}
	result := evaluateForTest(t, "${{ false && env.unused }}", context)
	if result.Value != false || resolved != 0 {
		t.Fatalf("result = %#v, resolved properties = %d", result.Value, resolved)
	}
	result = evaluateForTest(t, "${{ env.TARGET }}", context)
	if result.Value != "main" || resolved != 1 {
		t.Fatalf("result = %#v, resolved properties = %d", result.Value, resolved)
	}
}

func TestDeferredObjectMapEnumeratesValues(t *testing.T) {
	loaded := 0
	context := Context{
		Availability: NewAvailability("vars"),
		Values: map[string]any{
			"vars": DeferredObjectMap{
				Resolve: func(name string) (any, bool, error) {
					return nil, false, nil
				},
				Values: func() (map[string]any, error) {
					loaded++
					return map[string]any{"ENVIRONMENT": "production", "RUNNER": "ubuntu-latest"}, nil
				},
			},
		},
	}
	result := evaluateForTest(t, "${{ toJSON(vars) }}", context)
	want := "{\n  \"ENVIRONMENT\": \"production\",\n  \"RUNNER\": \"ubuntu-latest\"\n}"
	if result.Value != want || loaded != 1 {
		t.Fatalf("result = %#v, loads = %d", result, loaded)
	}
}

func TestFunctions(t *testing.T) {
	status := &Status{Failure: true}
	context := Context{
		Availability: NewAvailability("github").WithStatusFunctions(),
		Values: map[string]any{
			"github": map[string]any{"labels": []any{"bug", "help wanted"}, "ref": "refs/tags/v1.2.3"},
		},
		Status: status,
	}
	tests := []struct {
		expression string
		want       any
	}{
		{expression: "contains('Hello World', 'world')", want: true},
		{expression: "contains(github.labels, 'BUG')", want: true},
		{expression: "startsWith(github.ref, 'REFS/TAGS/V')", want: true},
		{expression: "endsWith(github.ref, 'V1.2.3')", want: true},
		{expression: "format('{0}/{1}', 'kelos-dev', 'kelos')", want: "kelos-dev/kelos"},
		{expression: "join(github.labels)", want: "bug,help wanted"},
		{expression: "join(github.labels, ' | ')", want: "bug | help wanted"},
		{expression: "join(fromJSON('[{\"name\":\"bug\"},[1]]'))", want: "Object,Array"},
		{expression: "fromJSON('[1, 2, 3]')[1]", want: float64(2)},
		{expression: "toJSON(fromJSON('{\"enabled\":true,\"retries\":2}'))", want: "{\n  \"enabled\": true,\n  \"retries\": 2\n}"},
		{expression: "success()", want: false},
		{expression: "always()", want: true},
		{expression: "failure()", want: true},
		{expression: "cancelled()", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			program, err := ParseCondition(tt.expression)
			if err != nil {
				t.Fatal(err)
			}
			result, err := program.Evaluate(context)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(result.Value) != fmt.Sprint(tt.want) {
				t.Fatalf("value = %#v, want %#v", result.Value, tt.want)
			}
		})
	}
}

func TestObjectFilters(t *testing.T) {
	context := Context{
		Availability: NewAvailability("github"),
		Values: map[string]any{
			"github": map[string]any{
				"labels": []any{
					map[string]any{"name": "bug"},
					map[string]any{"color": "red"},
					map[string]any{"name": "help wanted"},
				},
				"groups": map[string]any{
					"first":  map[string]any{"members": []any{map[string]any{"name": "alice"}}},
					"second": map[string]any{"members": []any{map[string]any{"name": "bob"}}},
				},
			},
		},
	}
	tests := []struct {
		expression string
		want       any
	}{
		{expression: "github.labels.*.name", want: []any{"bug", "help wanted"}},
		{expression: "github.labels[*].name", want: []any{"bug", "help wanted"}},
		{expression: "github.groups.*.members.*.name", want: []any{"alice", "bob"}},
		{expression: "join(github.labels.*.name, ', ')", want: "bug, help wanted"},
		{expression: "github.missing.*.name", want: []any{}},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			result := evaluateForTest(t, "${{ "+tt.expression+" }}", context)
			if !reflect.DeepEqual(result.Value, tt.want) {
				t.Fatalf("value = %#v, want %#v", result.Value, tt.want)
			}
		})
	}
	secretResult := evaluateForTest(t, "${{ github.labels.*.name }}", Context{
		Availability: NewAvailability("github"),
		Values: map[string]any{
			"github": map[string]any{"labels": []any{map[string]any{"name": Secret("private")}}},
		},
	})
	if !secretResult.Secret {
		t.Fatal("object filter discarded sensitivity")
	}
}

func TestHashFiles(t *testing.T) {
	workspace := t.TempDir()
	writeExpressionTestFile(t, filepath.Join(workspace, "a.txt"), "alpha")
	writeExpressionTestFile(t, filepath.Join(workspace, "sub", "b.txt"), "beta")
	writeExpressionTestFile(t, filepath.Join(workspace, "vendor", "c.txt"), "vendor")
	writeExpressionTestFile(t, filepath.Join(workspace, "ignored.log"), "ignored")
	context := Context{
		Availability: NewAvailability("github").WithHashFiles(),
		Values:       map[string]any{"github": map[string]any{"workspace": workspace}},
	}
	tests := []struct {
		expression string
		want       string
	}{
		{expression: "hashFiles('**/*.txt')", want: combinedFileHash("alpha", "beta", "vendor")},
		{expression: "hashFiles('**/*.txt', '!vendor/**')", want: combinedFileHash("alpha", "beta")},
		{expression: "hashFiles('sub')", want: combinedFileHash("beta")},
		{expression: "hashFiles('.')", want: combinedFileHash("alpha", "ignored", "beta", "vendor")},
		{expression: fmt.Sprintf("hashFiles('%s')", filepath.ToSlash(filepath.Join(workspace, "sub", "b.txt"))), want: combinedFileHash("beta")},
		{expression: "hashFiles('missing/**')", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			result := evaluateForTest(t, "${{ "+tt.expression+" }}", context)
			if result.Value != tt.want {
				t.Fatalf("value = %#v, want %#v", result.Value, tt.want)
			}
		})
	}

	program, err := Parse("${{ hashFiles('**') }}")
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Evaluate(Context{
		Availability: NewAvailability("github"),
		Values:       map[string]any{"github": map[string]any{"workspace": workspace}},
	})
	if err == nil || !strings.Contains(err.Error(), `function "hashFiles" is unavailable`) {
		t.Fatalf("error = %v, want unavailable hashFiles function", err)
	}

	program, err = Parse("${{ hashFiles('../secrets/**') }}")
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Evaluate(context)
	if err == nil || !strings.Contains(err.Error(), "must remain within github.workspace") {
		t.Fatalf("error = %v, want workspace escape rejection", err)
	}
}

func TestHashFilesMatchesAbsolutePatternThroughWorkspaceSymlink(t *testing.T) {
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	if err := os.Mkdir(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	writeExpressionTestFile(t, filepath.Join(workspace, "dependency.lock"), "locked dependency")
	workspaceLink := filepath.Join(parent, "workspace-link")
	if err := os.Symlink(workspace, workspaceLink); err != nil {
		t.Skipf("create workspace symlink: %v", err)
	}
	context := Context{
		Availability: NewAvailability("github").WithHashFiles(),
		Values:       map[string]any{"github": map[string]any{"workspace": workspaceLink}},
	}
	pattern := filepath.ToSlash(filepath.Join(workspaceLink, "dependency.lock"))
	result := evaluateForTest(t, "${{ hashFiles('"+pattern+"') }}", context)
	if result.Value != combinedFileHash("locked dependency") {
		t.Fatalf("value = %#v, want hash through workspace symlink", result.Value)
	}
}

func TestContextAndFunctionAvailabilityIsValidatedBeforeEvaluation(t *testing.T) {
	program, err := ParseCondition("false && secrets.TOKEN")
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Evaluate(Context{Availability: NewAvailability("github")})
	if err == nil || !strings.Contains(err.Error(), `context "secrets" is unavailable`) {
		t.Fatalf("error = %v, want unavailable secrets context", err)
	}

	program, err = ParseCondition("always()")
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Evaluate(Context{Availability: NewAvailability("github")})
	if err == nil || !strings.Contains(err.Error(), `function "always" is unavailable`) {
		t.Fatalf("error = %v, want unavailable status function", err)
	}

	program, err = ParseCondition("vars.NAME")
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Evaluate(Context{Availability: NewAvailability("vars")})
	if err == nil || !strings.Contains(err.Error(), `context "vars" is unavailable`) {
		t.Fatalf("error = %v, want missing runtime context", err)
	}
}

func TestMissingPropertiesAndIndexAccess(t *testing.T) {
	context := Context{
		Availability: NewAvailability("github"),
		Values: map[string]any{
			"github": map[string]any{"values": []any{"zero", "one"}, "sha": "abc"},
		},
	}
	tests := []struct {
		expression string
		want       any
	}{
		{expression: "github['sha']", want: "abc"},
		{expression: "github.values[1]", want: "one"},
		{expression: "github.missing", want: ""},
		{expression: "github.missing.child", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.expression, func(t *testing.T) {
			program, err := ParseCondition(tt.expression)
			if err != nil {
				t.Fatal(err)
			}
			result, err := program.Evaluate(context)
			if err != nil {
				t.Fatal(err)
			}
			if result.Value != tt.want {
				t.Fatalf("value = %#v, want %#v", result.Value, tt.want)
			}
		})
	}
}

func TestSecretDerivedValuesAreMarkedAndDiagnosticsAreRedacted(t *testing.T) {
	context := Context{
		Availability: NewAvailability("secrets"),
		Values: map[string]any{
			"secrets": map[string]any{"TOKEN": "super-secret", "EMPTY": "", "FORMAT": "{9}"},
		},
	}
	result := evaluateForTest(t, "prefix-${{ format('{0}', secrets.TOKEN) }}", context)
	if !result.Secret || result.Redacted() != "***" {
		t.Fatalf("result = %#v, want redacted secret", result)
	}
	text, err := result.String()
	if err != nil {
		t.Fatal(err)
	}
	if text != "prefix-super-secret" {
		t.Fatalf("text = %q", text)
	}

	program, err := Parse("${{ format(secrets.FORMAT, secrets.TOKEN) }}")
	if err != nil {
		t.Fatal(err)
	}
	_, err = program.Evaluate(context)
	if err == nil {
		t.Fatal("Evaluate() accepted an invalid secret-derived format")
	}
	if strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), "{9}") {
		t.Fatalf("diagnostic exposed secret-derived content: %v", err)
	}

	for _, input := range []string{"${{ secrets.TOKEN && 'enabled' }}", "${{ secrets.EMPTY || 'fallback' }}"} {
		result := evaluateForTest(t, input, context)
		if !result.Secret || result.Redacted() != "***" {
			t.Fatalf("result for %q = %#v, want redacted secret", input, result)
		}
	}

	for _, input := range []string{
		"${{ join(fromJSON(secrets.TOKEN), ',') }}",
		"${{ toJSON(secrets) }}",
		"${{ fromJSON(secrets.TOKEN) }}",
	} {
		result := evaluateForTest(t, input, Context{
			Availability: NewAvailability("secrets"),
			Values:       map[string]any{"secrets": map[string]any{"TOKEN": `["first","second"]`}},
		})
		if !result.Secret || result.Redacted() != "***" {
			t.Fatalf("result for %q = %#v, want redacted secret", input, result)
		}
	}
}

func TestFromJSONRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{"not-json", "true false"} {
		program, err := ParseCondition("fromJSON('" + input + "')")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := program.Evaluate(Context{Availability: NewAvailability()}); err == nil {
			t.Fatalf("fromJSON(%q) succeeded", input)
		}
	}
}

func TestRejectsExcessiveExpressionComplexity(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "nesting",
			input: strings.Repeat("(", maxExpressionNesting+1) + "true" + strings.Repeat(")", maxExpressionNesting+1),
		},
		{
			name:  "nodes",
			input: strings.Repeat("true || ", maxExpressionNodes) + "true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseCondition(tt.input); err == nil {
				t.Fatal("ParseCondition() accepted excessive expression complexity")
			}
		})
	}
}

func TestRejectsOversizedEvaluationStrings(t *testing.T) {
	nestedFormat := "'x'"
	for range 17 {
		nestedFormat = "format('{0}{0}', " + nestedFormat + ")"
	}
	tests := []struct {
		name    string
		input   string
		context Context
	}{
		{name: "format amplification", input: "${{ " + nestedFormat + " }}", context: Context{Availability: NewAvailability()}},
		{
			name:  "template interpolation",
			input: "${{ github.value }}${{ github.value }}",
			context: Context{
				Availability: NewAvailability("github"),
				Values:       map[string]any{"github": map[string]any{"value": strings.Repeat("x", 60_000)}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			program, err := Parse(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := program.Evaluate(tt.context); err == nil || !strings.Contains(err.Error(), "evaluated expression exceeds") {
				t.Fatalf("error = %v, want evaluated expression size limit", err)
			}
		})
	}
}

func TestRejectsUnsupportedOrMalformedSyntax(t *testing.T) {
	tests := []string{
		"${{ github.sha",
		"${{ ${{ github.sha }} }}",
		"${{ github.sha + 'suffix' }}",
		"${{ \"double quoted\" }}",
		"${{ unknown(github.sha) }}",
		"${{ * }}",
		"${{ }}",
	}
	availability := NewAvailability("github")
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			program, err := Parse(input)
			if err == nil {
				err = program.Validate(availability)
			}
			if err == nil {
				t.Fatal("expression was accepted")
			}
		})
	}
}

func TestKelosExpressionPatterns(t *testing.T) {
	status := &Status{Success: true}
	availability := NewAvailability("github", "inputs", "vars", "secrets", "steps", "needs", "matrix", "env").WithStatusFunctions()
	context := Context{
		Availability: availability,
		Status:       status,
		Values: map[string]any{
			"github": map[string]any{
				"workflow": "CI", "head_ref": "", "ref_name": "v1.2.3", "ref": "refs/tags/v1.2.3",
				"sha": "merge-sha", "event_name": "pull_request", "repository": "kelos-dev/kelos",
				"run_id": 42, "run_attempt": 1, "server_url": "https://github.com", "actor": "octocat",
				"token": Secret("github-token"), "workspace": "/workspace",
				"event": map[string]any{
					"action": "opened", "comment": map[string]any{"body": "/kind bug"},
					"review": map[string]any{"body": ""}, "issue": map[string]any{"number": 117, "body": "/triage"},
					"pull_request": map[string]any{
						"body": "", "html_url": "https://github.com/kelos-dev/kelos/pull/24",
						"head":   map[string]any{"sha": "head-sha", "repo": map[string]any{"full_name": "kelos-dev/kelos"}},
						"number": 24,
					},
					"merge_group":  map[string]any{"head_sha": "merge-group-sha"},
					"workflow_run": map[string]any{"head_sha": "workflow-run-sha", "conclusion": "success"},
					"release":      map[string]any{"tag_name": "v1.2.3"},
				},
			},
			"inputs": map[string]any{
				"namespace": "", "use-environment-secrets": false, "checkout-ref": "",
				"persist-credentials": true, "allow-unsafe-pr-checkout": true,
				"expected-sha": "", "expected-head-sha": "head-sha", "environment-name": "ok-to-test",
			},
			"vars": map[string]any{"KELOS_NAMESPACE": ""},
			"secrets": map[string]any{
				"DEV_CLUSTER_KUBECONFIG": "kubeconfig", "CLOUDFLARE_ACCESS_CLIENT_ID": "client",
				"CLOUDFLARE_ACCESS_CLIENT_SECRET": "client-secret", "KELOS_WEBHOOK_SECRET": "webhook",
				"KELOS_GITHUB_APP_ID": "1", "KELOS_GITHUB_APP_INSTALLATION_ID": "2",
				"KELOS_GITHUB_APP_PRIVATE_KEY": "private-key", "KELOS_SESSION_TOKEN": "session",
				"GITHUB_TOKEN": "token", "CLAUDE_CODE_OAUTH_TOKEN": "claude", "CODEX_AUTH_JSON": "codex",
				"KELOS_SECRET_MANAGER_APP_ID": "3", "KELOS_SECRET_MANAGER_PRIVATE_KEY": "manager-key",
				"E2E_SKILLS_GITHUB_TOKEN": "e2e", "TAP_GITHUB_TOKEN": "tap",
			},
			"steps": map[string]any{
				"context":              map[string]any{"outputs": map[string]any{"has_command": "true", "body": "/kind bug", "issue_number": "24"}},
				"app-token":            map[string]any{"outputs": map[string]any{"token": "app-token"}},
				"version":              map[string]any{"outputs": map[string]any{"version": "v1.2.3"}},
				"private-skills-auth":  map[string]any{"outputs": map[string]any{"enabled": "true"}},
				"private-skills-token": map[string]any{"outputs": map[string]any{"token": "private-token"}},
				"create_task":          map[string]any{"outputs": map[string]any{"action": "created", "task_name": "task-1"}},
			},
			"needs": map[string]any{
				"test-e2e": map[string]any{"result": "success"}, "fork-e2e": map[string]any{"result": "success"},
				"comment-label":     map[string]any{"result": "success"},
				"determine-version": map[string]any{"outputs": map[string]any{"version": "v1.2.3"}},
			},
			"matrix": map[string]any{"arch": "arm64"},
			"env": map[string]any{
				"GCP_WORKLOAD_IDENTITY_PROVIDER": "provider", "GCP_SERVICE_ACCOUNT_EMAIL": "account",
				"GKE_CLUSTER_NAME": "cluster", "GKE_CLUSTER_LOCATION": "location", "GCP_PROJECT_ID": "project",
			},
		},
	}

	patterns := []string{
		"github.workflow",
		"github.head_ref || github.ref_name",
		"github.event.pull_request.head.sha || github.event.merge_group.head_sha",
		"github.event.pull_request.html_url || format('{0}/{1}/actions/runs/{2}', github.server_url, github.repository, github.run_id)",
		"github.run_id", "github.run_attempt", "github.repository", "github.server_url",
		"needs.test-e2e.result", "vars.KELOS_NAMESPACE || 'default'",
		"github.event.workflow_run.head_sha || github.sha",
		"secrets.DEV_CLUSTER_KUBECONFIG", "secrets.CLOUDFLARE_ACCESS_CLIENT_ID", "secrets.CLOUDFLARE_ACCESS_CLIENT_SECRET",
		"secrets.KELOS_WEBHOOK_SECRET", "secrets.KELOS_GITHUB_APP_ID", "secrets.KELOS_GITHUB_APP_INSTALLATION_ID",
		"secrets.KELOS_GITHUB_APP_PRIVATE_KEY", "secrets.KELOS_SESSION_TOKEN",
		"failure()", "always()", "github.event.issue.number == 117",
		"github.event.pull_request.number", "needs.fork-e2e.result",
		"steps.context.outputs.has_command == 'true'", "steps.context.outputs.body", "steps.context.outputs.issue_number",
		"always() && !cancelled() && (needs.comment-label.result == 'success' || needs.comment-label.result == 'skipped')",
		"always() && github.event.pull_request", "steps.app-token.outputs.token", "steps.version.outputs.version",
		"matrix.arch == 'arm64' && 'ubuntu-24.04-arm' || 'ubuntu-latest'", "github.actor", "secrets.GITHUB_TOKEN",
		"needs.determine-version.outputs.version", "startsWith(github.ref, 'refs/tags/v')", "github.token", "github.ref_name",
		"!inputs.use-environment-secrets", "inputs.checkout-ref == ''", "inputs.checkout-ref != ''",
		"inputs.persist-credentials", "inputs.allow-unsafe-pr-checkout", "inputs.expected-sha != ''", "inputs.expected-head-sha != ''",
		"steps.private-skills-auth.outputs.enabled == 'true'", "steps.private-skills-token.outputs.token || secrets.E2E_SKILLS_GITHUB_TOKEN",
		"inputs.use-environment-secrets", "inputs.environment-name", "github.workspace",
		"inputs.namespace || vars.KELOS_NAMESPACE || 'default'", "env.GCP_WORKLOAD_IDENTITY_PROVIDER",
		"env.GCP_SERVICE_ACCOUNT_EMAIL", "env.GKE_CLUSTER_NAME", "env.GKE_CLUSTER_LOCATION", "env.GCP_PROJECT_ID",
		"steps.create_task.outputs.action", "steps.create_task.outputs.task_name", "secrets.TAP_GITHUB_TOKEN",
		"github.event.release.tag_name",
		"(github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository) || github.event_name == 'merge_group'",
		"github.event_name == 'push' || github.event_name == 'merge_group' || (github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository)",
		"contains(github.event.comment.body || '', '/kind') || contains(github.event.review.body || '', '/priority') || contains(github.event.issue.body || '', '/triage')",
	}
	for _, pattern := range patterns {
		t.Run(pattern, func(t *testing.T) {
			program, err := ParseCondition(pattern)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := program.Evaluate(context); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func evaluateForTest(t *testing.T, input string, context Context) Result {
	t.Helper()
	program, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	result, err := program.Evaluate(context)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func writeExpressionTestFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func combinedFileHash(contents ...string) string {
	result := sha256.New()
	for _, content := range contents {
		digest := sha256.Sum256([]byte(content))
		_, _ = result.Write(digest[:])
	}
	return fmt.Sprintf("%x", result.Sum(nil))
}
