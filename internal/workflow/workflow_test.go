package workflow

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
)

const minimalJob = "jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n"

func TestParseRemainingKelosTriggers(t *testing.T) {
	triggers := map[string]string{
		"workflow run":                "workflow_run:\n    workflows: [Release]\n    types: [completed]\n    branches: [main]",
		"workflow dispatch":           "workflow_dispatch:\n    inputs:\n      namespace:\n        description: Kubernetes namespace\n        required: false\n        default: ''",
		"issues":                      "issues:\n    types: [opened, edited, labeled, unlabeled]",
		"pull request target":         "pull_request_target:\n    types: [opened, synchronize, reopened]",
		"issue comment":               "issue_comment:\n    types: [created]",
		"pull request review comment": "pull_request_review_comment:\n    types: [created]",
		"pull request review":         "pull_request_review:\n    types: [submitted]",
		"schedule":                    "schedule:\n    - cron: '0 6 * * *'",
		"release":                     "release:\n    types: [published]",
		"workflow call":               "workflow_call:\n    inputs:\n      checkout-ref:\n        type: string\n        default: ''\n      persist-credentials:\n        type: boolean\n        default: true",
	}
	for name, trigger := range triggers {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte("name: CI\non:\n  " + trigger + "\n" + minimalJob)); err != nil {
				t.Fatalf("Parse() rejected trigger used by kelos: %v", err)
			}
		})
	}
}

func TestMatchWorkflowRunFilters(t *testing.T) {
	definition, err := Parse([]byte("name: Deploy\non:\n  workflow_run:\n    workflows: [Release]\n    types: [completed]\n    branches: [main]\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	if !Matches(definition.On, Event{Name: "workflow_run", Action: "completed", WorkflowName: "Release", BaseRef: "main"}) {
		t.Fatal("matching workflow_run event was rejected")
	}
	for _, event := range []Event{
		{Name: "workflow_run", Action: "requested", WorkflowName: "Release", BaseRef: "main"},
		{Name: "workflow_run", Action: "completed", WorkflowName: "CI", BaseRef: "main"},
		{Name: "workflow_run", Action: "completed", WorkflowName: "Release", BaseRef: "feature"},
	} {
		if Matches(definition.On, event) {
			t.Fatalf("workflow_run filters matched %#v", event)
		}
	}
}

func TestMatchPushBranchAndTagFilters(t *testing.T) {
	definition, err := Parse([]byte("name: Release\non:\n  push:\n    branches: [main]\n    tags: ['v*']\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []Event{
		{Name: "push", Ref: "refs/heads/main", RefName: "main"},
		{Name: "push", Ref: "refs/tags/v1.2.3", RefName: "v1.2.3"},
	} {
		if !Matches(definition.On, event) {
			t.Fatalf("matching push was rejected: %#v", event)
		}
	}
	for _, event := range []Event{
		{Name: "push", Ref: "refs/heads/feature", RefName: "feature"},
		{Name: "push", Ref: "refs/tags/canary", RefName: "canary"},
	} {
		if Matches(definition.On, event) {
			t.Fatalf("non-matching push was accepted: %#v", event)
		}
	}

	tagsOnly, err := Parse([]byte("name: Release\non:\n  push:\n    tags: ['v*']\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	if Matches(tagsOnly.On, Event{Name: "push", Ref: "refs/heads/main", RefName: "main"}) {
		t.Fatal("tags-only push filter matched a branch")
	}
}

func TestMatchWorkflowDispatchResolvesInputs(t *testing.T) {
	definition, err := Parse([]byte("name: Manual\non:\n  workflow_dispatch:\n    inputs:\n      namespace:\n        required: true\n        type: string\n      dry-run:\n        type: boolean\n        default: false\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	event, matched, err := Match(definition.On, Event{Name: "workflow_dispatch", Inputs: map[string]string{"namespace": "default"}})
	if err != nil || !matched {
		t.Fatalf("Match() = matched %v, error %v", matched, err)
	}
	if event.Inputs["namespace"] != "default" || event.Inputs["dry-run"] != "false" {
		t.Fatalf("resolved inputs = %#v", event.Inputs)
	}
	if event.InputValues["namespace"] != "default" || event.InputValues["dry-run"] != false {
		t.Fatalf("expression inputs = %#v", event.InputValues)
	}
	for _, inputs := range []map[string]string{
		{},
		{"namespace": "default", "unknown": "value"},
		{"namespace": "default", "dry-run": "not-a-boolean"},
		{"namespace": "default", "dry-run": "1"},
		{"namespace": "default", "dry-run": "TRUE"},
		{"namespace": strings.Repeat("界", maxInputValueLength+1)},
	} {
		if _, _, err := Match(definition.On, Event{Name: "workflow_dispatch", Inputs: inputs}); err == nil {
			t.Fatalf("Match() accepted inputs %#v", inputs)
		}
	}
}

func TestMatchWorkflowCallResolvesImplicitDefaults(t *testing.T) {
	definition, err := Parse([]byte("name: Reusable\non:\n  workflow_call:\n    inputs:\n      enabled:\n        type: boolean\n      retries:\n        type: number\n      label:\n        type: string\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	event, matched, err := Match(definition.On, Event{Name: "workflow_call"})
	if err != nil || !matched {
		t.Fatalf("Match() = matched %v, error %v", matched, err)
	}
	want := map[string]string{"enabled": "false", "retries": "0", "label": ""}
	if !maps.Equal(event.Inputs, want) {
		t.Fatalf("resolved inputs = %#v, want %#v", event.Inputs, want)
	}
	if event.InputValues["enabled"] != false || event.InputValues["retries"] != float64(0) || event.InputValues["label"] != "" {
		t.Fatalf("expression inputs = %#v", event.InputValues)
	}
}

func TestParseRejectsCaseInsensitiveDuplicateInputNames(t *testing.T) {
	for _, eventName := range []string{"workflow_dispatch", "workflow_call"} {
		t.Run(eventName, func(t *testing.T) {
			workflowText := fmt.Sprintf("name: Inputs\non:\n  %s:\n    inputs:\n      Target:\n        type: string\n      target:\n        type: string\n%s", eventName, minimalJob)
			if _, err := Parse([]byte(workflowText)); err == nil {
				t.Fatal("Parse() accepted input names that differ only by case")
			}
		})
	}
}

func TestTypedInputLexicalForms(t *testing.T) {
	definition, err := Parse([]byte("name: Reusable\non:\n  workflow_call:\n    inputs:\n      enabled:\n        type: boolean\n      amount:\n        type: number\n" + minimalJob))
	if err != nil {
		t.Fatal(err)
	}
	if _, matched, err := Match(definition.On, Event{Name: "workflow_call", Inputs: map[string]string{"enabled": "true", "amount": "-1.5e+2"}}); err != nil || !matched {
		t.Fatalf("Match() rejected canonical typed inputs: matched %v, error %v", matched, err)
	}
	for _, inputs := range []map[string]string{
		{"enabled": "1"},
		{"enabled": "t"},
		{"enabled": "TRUE"},
		{"amount": "NaN"},
		{"amount": "Inf"},
		{"amount": "+1"},
		{"amount": ".5"},
		{"amount": "01"},
	} {
		if _, _, err := Match(definition.On, Event{Name: "workflow_call", Inputs: inputs}); err == nil {
			t.Fatalf("Match() accepted inputs %#v", inputs)
		}
	}
}

func TestInputPayloadBoundary(t *testing.T) {
	workflowText := "name: Manual\non:\n  workflow_dispatch:\n    inputs:\n      a:\n        type: string\n" + minimalJob
	definition, err := Parse([]byte(workflowText))
	if err != nil {
		t.Fatal(err)
	}
	if _, matched, err := Match(definition.On, Event{Name: "workflow_dispatch", Inputs: map[string]string{"a": strings.Repeat("x", maxInputPayloadLength-1)}}); err != nil || !matched {
		t.Fatalf("Match() rejected input payload at the maximum: matched %v, error %v", matched, err)
	}
	if _, _, err := Match(definition.On, Event{Name: "workflow_dispatch", Inputs: map[string]string{"a": strings.Repeat("x", maxInputPayloadLength)}}); err == nil {
		t.Fatal("Match() accepted input payload above the maximum")
	}

	atLimit := fmt.Sprintf("name: Manual\non:\n  workflow_dispatch:\n    inputs:\n      a:\n        type: string\n        default: '%s'\n%s", strings.Repeat("x", maxInputPayloadLength-1), minimalJob)
	if _, err := Parse([]byte(atLimit)); err != nil {
		t.Fatalf("Parse() rejected default input payload at the maximum: %v", err)
	}
	aboveLimit := fmt.Sprintf("name: Manual\non:\n  workflow_dispatch:\n    inputs:\n      a:\n        type: string\n        default: '%s'\n%s", strings.Repeat("x", maxInputPayloadLength), minimalJob)
	if _, err := Parse([]byte(aboveLimit)); err == nil {
		t.Fatal("Parse() accepted default input payload above the maximum")
	}
}

func TestParseAndMatchCron(t *testing.T) {
	schedule, err := ParseCron("0 6 * * *")
	if err != nil {
		t.Fatal(err)
	}
	if !ScheduleMatches(schedule, time.Date(2026, 8, 10, 6, 0, 30, 0, time.UTC)) {
		t.Fatal("schedule did not match its minute")
	}
	if ScheduleMatches(schedule, time.Date(2026, 8, 10, 6, 1, 0, 0, time.UTC)) {
		t.Fatal("schedule matched a different minute")
	}
	for _, expression := range []string{"", "* * * * *", "1,2 4 * * *", "@daily", "0 0 * *"} {
		if _, err := ParseCron(expression); err == nil {
			t.Fatalf("ParseCron() accepted %q", expression)
		}
	}
	_, err = ParseCron(strings.Repeat("é", maxCronLength-8) + " 0 0 0 0")
	if err == nil || strings.Contains(err.Error(), "1 to 256 characters") {
		t.Fatalf("maximum-length cron error = %v", err)
	}
	_, err = ParseCron(strings.Repeat("é", maxCronLength-7) + " 0 0 0 0")
	if err == nil || !strings.Contains(err.Error(), "1 to 256 characters") {
		t.Fatalf("oversized cron error = %v", err)
	}
}

func TestSupportsEventAction(t *testing.T) {
	for _, tt := range []struct {
		name      string
		eventName string
		action    string
		want      bool
	}{
		{name: "push", eventName: "push", want: true},
		{name: "issue opened", eventName: "issues", action: "opened", want: true},
		{name: "unknown issue activity", eventName: "issues", action: "future_activity"},
		{name: "push action", eventName: "push", action: "created"},
		{name: "unknown event", eventName: "future_event"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := SupportsEventAction(tt.eventName, tt.action); got != tt.want {
				t.Fatalf("SupportsEventAction(%q, %q) = %v, want %v", tt.eventName, tt.action, got, tt.want)
			}
		})
	}
}

func TestParseCIWorkflow(t *testing.T) {
	data, err := os.ReadFile("testdata/ci.yaml")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := Parse(data)
	if err != nil {
		t.Fatalf("parse workflow: %v", err)
	}
	if len(definition.Jobs) != 3 {
		t.Fatalf("jobs = %d, want 3", len(definition.Jobs))
	}
	for _, id := range []string{"build", "verify", "test"} {
		if len(definition.Jobs[id].Steps) != 3 {
			t.Errorf("job %q steps = %d, want 3", id, len(definition.Jobs[id].Steps))
		}
	}
}

func TestParseDogfoodCIWorkflow(t *testing.T) {
	data, err := os.ReadFile("../../.open-actions/workflows/ci.yaml")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := Parse(data)
	if err != nil {
		t.Fatalf("parse dogfood workflow: %v", err)
	}
	if len(definition.Jobs) != 3 {
		t.Fatalf("jobs = %d, want 3", len(definition.Jobs))
	}
}

func TestParseDogfoodE2EWorkflow(t *testing.T) {
	data, err := os.ReadFile("../../.open-actions/workflows/e2e.yaml")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := Parse(data)
	if err != nil {
		t.Fatalf("parse dogfood e2e workflow: %v", err)
	}
	if !Matches(definition.On, Event{Name: "push", Ref: "refs/heads/main", RefName: "main"}) {
		t.Error("dogfood e2e workflow does not match a main push")
	}
	if Matches(definition.On, Event{Name: "pull_request", Action: "synchronize", BaseRef: "main"}) {
		t.Error("dogfood e2e workflow matches a pull request")
	}
	job, found := definition.Jobs["test-e2e"]
	if !found {
		t.Fatal("dogfood e2e workflow does not define test-e2e")
	}
	if got, want := []string(job.RunsOn), []string{"self-hosted", "linux", "x64", "docker"}; !slices.Equal(got, want) {
		t.Errorf("runs-on = %v, want %v", got, want)
	}
	if len(job.Steps) < 3 || job.Steps[2].Uses != "helm/kind-action@v1" {
		t.Errorf("kind action step = %#v", job.Steps)
	}
}

func TestParseRejectsTrailingYAMLDocument(t *testing.T) {
	data := []byte("name: CI\non: push\njobs:\n  build:\n    runs-on: linux\n    steps:\n      - run: true\n---\nname: ignored\n")
	if _, err := Parse(data); err == nil {
		t.Fatal("Parse() accepted a trailing YAML document")
	}
}

func TestMatches(t *testing.T) {
	data, err := os.ReadFile("testdata/ci.yaml")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		event Event
		want  bool
	}{
		{name: "push main", event: Event{Name: "push", Ref: "refs/heads/main", RefName: "main"}, want: true},
		{name: "push branch", event: Event{Name: "push", Ref: "refs/heads/feature", RefName: "feature"}},
		{name: "push tag with branch name", event: Event{Name: "push", Ref: "refs/tags/main", RefName: "main"}},
		{name: "pull request", event: Event{Name: "pull_request", Action: "synchronize", BaseRef: "main"}, want: true},
		{name: "pull request action", event: Event{Name: "pull_request", Action: "closed", BaseRef: "main"}},
		{name: "merge group", event: Event{Name: "merge_group", Action: "checks_requested", BaseRef: "main"}, want: true},
		{name: "merge group destroyed", event: Event{Name: "merge_group", Action: "destroyed", BaseRef: "main"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Matches(definition.On, tt.event); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesDefaultMergeGroupTypes(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non: merge_group\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !Matches(definition.On, Event{Name: "merge_group", Action: "checks_requested"}) {
		t.Error("default merge_group types did not match checks_requested")
	}
	if Matches(definition.On, Event{Name: "merge_group", Action: "destroyed"}) {
		t.Error("default merge_group types matched destroyed")
	}
}

func TestMatchesDefaultPullRequestTypes(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"opened", "synchronize", "reopened"} {
		if !Matches(definition.On, Event{Name: "pull_request", Action: action}) {
			t.Errorf("default pull_request types did not match %q", action)
		}
	}
	for _, action := range []string{"closed", "labeled"} {
		if Matches(definition.On, Event{Name: "pull_request", Action: action}) {
			t.Errorf("default pull_request types matched %q", action)
		}
	}
}

func TestMatchesExplicitPullRequestTypes(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non:\n  pull_request:\n    types: [closed]\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !Matches(definition.On, Event{Name: "pull_request", Action: "closed"}) {
		t.Error("explicit pull_request type did not match")
	}
	if Matches(definition.On, Event{Name: "pull_request", Action: "opened"}) {
		t.Error("explicit pull_request types used the default activity set")
	}
}

func TestParseRejectsEmptyPullRequestTypes(t *testing.T) {
	data := []byte("name: CI\non:\n  pull_request:\n    types: []\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n")
	if _, err := Parse(data); err == nil {
		t.Fatal("Parse() accepted an empty pull_request activity filter")
	}
}

func TestParseRejectsEmptyBranchFilters(t *testing.T) {
	data := []byte("name: CI\non:\n  push:\n    branches: []\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n")
	if _, err := Parse(data); err == nil {
		t.Fatal("Parse() accepted an empty branch filter")
	}
}

func TestParseRejectsConcurrencyWithoutGroup(t *testing.T) {
	for _, concurrency := range []string{"''", "{}", "{cancel-in-progress: true}"} {
		t.Run(concurrency, func(t *testing.T) {
			data := []byte("name: CI\non: push\nconcurrency: " + concurrency + "\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n")
			if _, err := Parse(data); err == nil {
				t.Fatal("Parse() accepted concurrency without a group")
			}
		})
	}
}

func TestEvaluateConcurrency(t *testing.T) {
	data, err := os.ReadFile("testdata/ci.yaml")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	group, cancel, err := EvaluateConcurrency(definition, Event{RefName: "42/merge", HeadRef: "feature"})
	if err != nil {
		t.Fatal(err)
	}
	if group != "CI-feature" || !cancel {
		t.Errorf("group = %q, cancel = %v", group, cancel)
	}
}

func TestEvaluateConcurrencyUsesRefNameFallback(t *testing.T) {
	definition := &Definition{
		Name:        "CI",
		Concurrency: Concurrency{Group: "${{ github.head_ref || github.ref_name }}"},
	}
	group, _, err := EvaluateConcurrency(definition, Event{Name: "push", RefName: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if group != "main" {
		t.Errorf("group = %q, want main", group)
	}
}

func TestEvaluateConcurrencyUsesInputs(t *testing.T) {
	definition := &Definition{
		Name:        "Deploy",
		Concurrency: Concurrency{Group: "deploy-${{ inputs.environment }}-${{ github.event.inputs.environment }}"},
	}
	group, _, err := EvaluateConcurrency(definition, Event{Inputs: map[string]string{"environment": "staging"}, InputValues: map[string]any{"environment": "staging"}})
	if err != nil {
		t.Fatal(err)
	}
	if group != "deploy-staging-staging" {
		t.Errorf("group = %q, want deploy-staging-staging", group)
	}
}

func TestEvaluateConcurrencyUsesPullRequestMetadata(t *testing.T) {
	definition := &Definition{
		Name:        "Fork E2E",
		Concurrency: Concurrency{Group: "fork-e2e-${{ github.event.pull_request.number }}-${{ github.event.pull_request.head.repo.full_name }}-${{ github.event.pull_request.head.sha }}-${{ github.event.pull_request.merge_ref }}"},
	}
	event := Event{
		Name: "pull_request_target", HeadRef: "feature", BaseRef: "main",
		PullRequest: &PullRequest{
			Number: 42, HeadRef: "feature", HeadSHA: strings.Repeat("a", 40), BaseRef: "main",
			HeadRepository: Repository{ID: 2, Owner: "contributor", Name: "example"},
		},
	}
	group, _, err := EvaluateConcurrency(definition, event)
	if err != nil {
		t.Fatal(err)
	}
	want := "fork-e2e-42-contributor/example-" + strings.Repeat("a", 40) + "-refs/pull/42/merge"
	if group != want {
		t.Errorf("group = %q, want %q", group, want)
	}
}

func TestEvaluateConcurrencyUsesPullRequestMergeRevision(t *testing.T) {
	definition := &Definition{Name: "CI", Concurrency: Concurrency{Group: "pr-${{ github.event.pull_request.merge_commit_sha }}"}}
	sha := strings.Repeat("a", 40)
	group, _, err := EvaluateConcurrency(definition, Event{Name: "pull_request", SHA: sha, PullRequest: &PullRequest{Number: 42}})
	if err != nil {
		t.Fatal(err)
	}
	if group != "pr-"+sha {
		t.Fatalf("group = %q, want %q", group, "pr-"+sha)
	}
}

func TestEvaluateConcurrencyUsesBoundedEventMetadata(t *testing.T) {
	definition := &Definition{
		Name:        "Events",
		Concurrency: Concurrency{Group: "${{ github.event.workflow_run.conclusion }}-${{ github.event.issue.number }}-${{ github.event.comment.body }}-${{ github.event.review.body }}-${{ github.event.release.tag_name }}"},
	}
	event := Event{
		Name:        "release",
		RefName:     "v1.2.3",
		WorkflowRun: &WorkflowRunEvent{Conclusion: "success", HeadSHA: strings.Repeat("a", 40)},
		Issue:       &IssueEvent{Number: 17, Body: "Issue body"},
		Comment:     &CommentEvent{Body: "comment"},
		Review:      &ReviewEvent{Body: "review"},
	}
	group, _, err := EvaluateConcurrency(definition, event)
	if err != nil {
		t.Fatal(err)
	}
	if group != "success-17-comment-review-v1.2.3" {
		t.Fatalf("group = %q", group)
	}
}

func TestEvaluateConcurrencyUsesEmptyStringForMissingProperty(t *testing.T) {
	definition := &Definition{Name: "CI", Concurrency: Concurrency{Group: "deploy-${{ github.head_ref }}"}}
	group, _, err := EvaluateConcurrency(definition, Event{Name: "push", RefName: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if group != "deploy-" {
		t.Fatalf("group = %q, want deploy-", group)
	}
}

func TestEvaluateConcurrencyRejectsEmptyEvaluatedGroup(t *testing.T) {
	definition := &Definition{Name: "CI", Concurrency: Concurrency{Group: "${{ github.head_ref }}"}}
	if _, _, err := EvaluateConcurrency(definition, Event{Name: "push", RefName: "main"}); err == nil {
		t.Fatal("EvaluateConcurrency() accepted an empty evaluated group")
	}
}

func TestParseAcceptsExternalAction(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: example/action/path@v1\n        with:\n          input: value\n"))
	if err != nil {
		t.Fatal(err)
	}
	if definition.Jobs["build"].Steps[0].Uses != "example/action/path@v1" {
		t.Errorf("action = %q", definition.Jobs["build"].Steps[0].Uses)
	}
}

func TestParseAcceptsRunsOnLabels(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: [self-hosted, Linux, ARM64]\n    steps:\n      - run: make test\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"self-hosted", "linux", "arm64"}
	got := []string(definition.Jobs["build"].RunsOn)
	if !slices.Equal(got, want) {
		t.Errorf("runs-on = %v, want %v", got, want)
	}
}

func TestEvaluateJobResolvesPlanningExpressions(t *testing.T) {
	job := Job{
		Name:   "Build ${{ github.ref_name }}",
		RunsOn: StringList{"${{ github.ref_name == 'main' && 'Ubuntu-Latest' || 'self-hosted' }}"},
	}
	main, err := EvaluateJob("build", job, workflowexpression.Context{
		Availability: workflowexpression.NewAvailability("github"),
		Values:       map[string]any{"github": map[string]any{"ref_name": "main"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	feature, err := EvaluateJob("build", job, workflowexpression.Context{
		Availability: workflowexpression.NewAvailability("github"),
		Values:       map[string]any{"github": map[string]any{"ref_name": "feature"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if main.Name != "Build main" || len(main.RunsOn) != 1 || main.RunsOn[0] != "ubuntu-latest" {
		t.Fatalf("main job = %#v", main)
	}
	if feature.Name != "Build feature" || len(feature.RunsOn) != 1 || feature.RunsOn[0] != "self-hosted" {
		t.Fatalf("feature job = %#v", feature)
	}
}

func TestEvaluateJobRejectsUnavailablePlanningContext(t *testing.T) {
	job := Job{RunsOn: StringList{"${{ matrix.arch }}"}}
	_, err := EvaluateJob("build", job, workflowexpression.Context{Availability: workflowexpression.NewAvailability("github")})
	if err == nil || !strings.Contains(err.Error(), `context "matrix" is unavailable`) {
		t.Fatalf("error = %v, want unavailable matrix context", err)
	}
}

func TestMatchesGitHubBranchPatterns(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		branch   string
		want     bool
	}{
		{name: "double star spans slash", patterns: []string{"releases/**"}, branch: "releases/beta/1", want: true},
		{name: "leading globstar matches zero segments", patterns: []string{"**/main"}, branch: "main", want: true},
		{name: "leading globstar matches nested segments", patterns: []string{"**/main"}, branch: "feature/main", want: true},
		{name: "middle globstar matches zero segments", patterns: []string{"release/**/stable"}, branch: "release/stable", want: true},
		{name: "middle globstar matches nested segments", patterns: []string{"release/**/stable"}, branch: "release/candidate/v1/stable", want: true},
		{name: "single star excludes slash", patterns: []string{"releases/*"}, branch: "releases/beta/1"},
		{name: "plus repeats prior class", patterns: []string{"v[12].[0-9]+.[0-9]+"}, branch: "v2.10.3", want: true},
		{name: "question mark makes prior character optional", patterns: []string{"*.jsx?"}, branch: "page.js", want: true},
		{name: "escaped operator is literal", patterns: []string{`feature\+api`}, branch: "feature+api", want: true},
		{name: "later negative pattern excludes", patterns: []string{"releases/**", "!releases/**-alpha"}, branch: "releases/beta/3-alpha"},
		{name: "later positive pattern includes", patterns: []string{"**", "!releases/**", "releases/stable"}, branch: "releases/stable", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesBranch(tt.patterns, tt.branch); got != tt.want {
				t.Errorf("matchesBranch(%v, %q) = %v, want %v", tt.patterns, tt.branch, got, tt.want)
			}
		})
	}
}

func TestParseAcceptsGitHubJobID(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non: push\njobs:\n  _Build-1:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, found := definition.Jobs["_Build-1"]; !found {
		t.Fatal("valid workflow job ID was not parsed")
	}
}

func TestParseRejectsInvalidJobID(t *testing.T) {
	for _, id := range []string{"1build", "build.test", strings.Repeat("a", maxJobIDLength+1)} {
		data := fmt.Sprintf("name: CI\non: push\njobs:\n  %s:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n", id)
		if _, err := Parse([]byte(data)); err == nil {
			t.Errorf("Parse() accepted workflow job ID %q", id)
		}
	}
}

func TestParseRejectsNonASCIIRunsOnLabel(t *testing.T) {
	_, err := Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: [self-hosted, Línux]\n    steps:\n      - run: make test\n"))
	if err == nil {
		t.Fatal("non-ASCII runs-on label was accepted")
	}
}

func TestParseRejectsInvalidActionReference(t *testing.T) {
	_, err := Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./local-action\n"))
	if err == nil {
		t.Fatal("Parse() accepted an invalid action reference")
	}
}

func TestParseRejectsUnknownField(t *testing.T) {
	_, err := Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    timeout-minutes: 10\n    steps:\n      - run: true\n"))
	if err == nil {
		t.Fatal("Parse() accepted an unsupported field")
	}
}

func TestParseAcceptsWorkflowExpressions(t *testing.T) {
	tests := []struct {
		name string
		job  string
	}{
		{name: "job environment", job: "    env:\n      VALUE: '${{ github.sha }}'\n    steps:\n      - run: echo test\n"},
		{name: "step environment", job: "    steps:\n      - run: echo test\n        env:\n          VALUE: '${{ github.sha }}'\n"},
		{name: "action input", job: "    steps:\n      - uses: actions/example@v1\n        with:\n          value: '${{ github.sha }}'\n"},
		{name: "run script", job: "    steps:\n      - run: 'echo ${{ github.sha }}'\n"},
		{name: "working directory", job: "    steps:\n      - run: echo test\n        working-directory: '${{ github.workspace }}'\n"},
		{name: "step condition", job: "    steps:\n      - run: echo test\n        if: github.ref_name == 'main'\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n" + tt.job)
			if _, err := Parse(data); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestParseRejectsInvalidOrUnavailableWorkflowExpressions(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{
			name: "unclosed expression",
			data: "name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: 'echo ${{ github.sha'\n",
		},
		{
			name: "secret in condition",
			data: "name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n        if: secrets.TOKEN\n",
		},
		{
			name: "secret in concurrency",
			data: "name: CI\non: push\nconcurrency: '${{ secrets.TOKEN }}'\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.data)); err == nil {
				t.Fatal("Parse() accepted an invalid workflow expression")
			}
		})
	}
}

func TestParseRejectsReservedEnvironmentVariables(t *testing.T) {
	tests := []string{
		"    env:\n      GITHUB_SHA: untrusted\n    steps:\n      - run: echo test\n",
		"    steps:\n      - run: echo test\n        env:\n          runner_os: Other\n",
	}
	for _, job := range tests {
		data := []byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n" + job)
		if _, err := Parse(data); err == nil {
			t.Fatal("Parse() accepted a reserved environment variable")
		}
	}
}

func TestParseRejectsUnknownTriggerField(t *testing.T) {
	_, err := Parse([]byte("name: CI\non:\n  push:\n    branches-ignore: [main]\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n"))
	if err == nil {
		t.Fatal("Parse() accepted an unsupported trigger field")
	}
}

func TestParseRejectsWithOnRunStep(t *testing.T) {
	data := []byte("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n        with:\n          value: ignored\n")
	if _, err := Parse(data); err == nil {
		t.Fatal("Parse() accepted with on a run step")
	}
}

func TestParseRejectsDuplicateCustomMappingKeys(t *testing.T) {
	for _, tt := range []struct {
		name string
		data string
	}{
		{
			name: "trigger event",
			data: "name: CI\non:\n  push:\n  push:\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n",
		},
		{
			name: "event filter",
			data: "name: CI\non:\n  push:\n    branches: [main]\n    branches: [feature]\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n",
		},
		{
			name: "concurrency",
			data: "name: CI\non: push\nconcurrency:\n  group: first\n  group: second\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.data)); err == nil {
				t.Fatal("Parse() accepted duplicate mapping keys")
			}
		})
	}
}

func TestParseRejectsUnsupportedTriggerConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		trigger string
	}{
		{name: "event", trigger: "schedule"},
		{name: "push activity type", trigger: "push:\n    types: [created]"},
		{name: "pull request activity type", trigger: "pull_request:\n    types: [typo]"},
		{name: "branch pattern", trigger: "push:\n    branches: ['[invalid']"},
		{name: "branch quantifier without atom", trigger: "push:\n    branches: ['?invalid']"},
		{name: "only negative branch patterns", trigger: "push:\n    branches: ['!main']"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte("name: CI\non:\n  " + tt.trigger + "\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n")
			if _, err := Parse(data); err == nil {
				t.Fatal("Parse() accepted unsupported trigger configuration")
			}
		})
	}
}

func TestParseNormalizesRunsOnLabels(t *testing.T) {
	definition, err := Parse([]byte("name: CI\non: push\njobs:\n  build:\n    runs-on: [Self-Hosted, LINUX]\n    steps:\n      - run: make test\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := []string(definition.Jobs["build"].RunsOn); !slices.Equal(got, []string{"self-hosted", "linux"}) {
		t.Fatalf("normalized runs-on = %v", got)
	}
}

func TestParseRejectsNonStringRunsOnLabels(t *testing.T) {
	for _, runsOn := range []string{"true", "123", "[self-hosted, true]", "[linux, 123]"} {
		data := []byte("name: CI\non: push\njobs:\n  build:\n    runs-on: " + runsOn + "\n    steps:\n      - run: make test\n")
		if _, err := Parse(data); err == nil {
			t.Fatalf("Parse() accepted runs-on: %s", runsOn)
		}
	}
}

func TestParseEnforcesWorkflowConfigurationBounds(t *testing.T) {
	base := "name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n"
	tests := []struct {
		name string
		data string
	}{
		{name: "workflow bytes", data: strings.Repeat("x", maxWorkflowBytes+1)},
		{name: "steps", data: base + strings.Repeat("      - run: true\n", maxSteps+1)},
		{name: "run script", data: base + "      - run: " + strings.Repeat("x", MaxRunScriptBytes+1) + "\n"},
		{name: "working directory", data: base + "      - run: true\n        working-directory: " + strings.Repeat("x", MaxWorkingDirectoryLength+1) + "\n"},
		{name: "step name", data: base + "      - name: " + strings.Repeat("x", MaxStepNameLength+1) + "\n        run: true\n"},
		{name: "condition", data: base + "      - if: ${{ '" + strings.Repeat("x", MaxConditionBytes+1) + "' }}\n        run: true\n"},
		{name: "aggregate job content", data: base + "      - run: " + strings.Repeat("x", 60_000) + "\n      - run: " + strings.Repeat("x", 60_000) + "\n"},
		{name: "condition aggregate content", data: base + "      - if: ${{ '" + strings.Repeat("x", 50_000) + "' }}\n        run: " + strings.Repeat("x", 60_000) + "\n"},
		{name: "branch patterns", data: workflowWithBranchPatterns(maxBranchPatterns + 1)},
		{name: "environment entries", data: workflowWithEnvironment(maxMapEntries + 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.data)); err == nil {
				t.Fatal("Parse() accepted configuration above its documented bound")
			}
		})
	}

	accepted := base + strings.Repeat("      - run: true\n", maxSteps)
	if _, err := Parse([]byte(accepted)); err != nil {
		t.Fatalf("Parse() rejected step count at the boundary: %v", err)
	}
}

func workflowWithBranchPatterns(count int) string {
	var builder strings.Builder
	builder.WriteString("name: CI\non:\n  push:\n    branches:\n")
	for index := 0; index < count; index++ {
		fmt.Fprintf(&builder, "      - branch-%d\n", index)
	}
	builder.WriteString("jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n")
	return builder.String()
}

func workflowWithEnvironment(count int) string {
	var builder strings.Builder
	builder.WriteString("name: CI\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    env:\n")
	for index := 0; index < count; index++ {
		fmt.Fprintf(&builder, "      KEY_%d: value\n", index)
	}
	builder.WriteString("    steps:\n      - run: true\n")
	return builder.String()
}

func TestParseRejectsMissingOrLongWorkflowName(t *testing.T) {
	for _, name := range []string{"", strings.Repeat("n", maxWorkflowNameLength+1)} {
		data := []byte("name: " + name + "\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo test\n")
		if _, err := Parse(data); err == nil {
			t.Errorf("Parse() accepted workflow name with %d characters", len(name))
		}
	}
}
