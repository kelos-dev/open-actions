package workflow

import (
	"strings"
	"testing"

	"github.com/kelos-dev/open-actions/internal/expression"
)

func TestExpressionAvailabilityMatchesGitHubContextTable(t *testing.T) {
	tests := []struct {
		name      string
		site      ExpressionSite
		contexts  string
		status    bool
		hashFiles bool
	}{
		{name: "workflow concurrency", site: ExpressionWorkflowConcurrency, contexts: "github inputs vars"},
		{name: "workflow environment", site: ExpressionWorkflowEnvironment, contexts: "github open_actions secrets inputs vars"},
		{name: "job condition", site: ExpressionJobCondition, contexts: "github open_actions needs vars inputs", status: true},
		{name: "job strategy", site: ExpressionJobStrategy, contexts: "github open_actions needs vars inputs"},
		{name: "job configuration", site: ExpressionJobConfiguration, contexts: "github open_actions needs strategy matrix vars inputs"},
		{name: "job environment", site: ExpressionJobEnvironment, contexts: "github open_actions needs strategy matrix vars secrets inputs"},
		{name: "job output", site: ExpressionJobOutput, contexts: "github open_actions needs strategy matrix job runner env vars secrets steps inputs"},
		{name: "step", site: ExpressionStep, contexts: "github open_actions needs strategy matrix job runner env vars secrets steps inputs", hashFiles: true},
		{name: "step condition", site: ExpressionStepCondition, contexts: "github open_actions needs strategy matrix job runner env vars steps inputs", status: true, hashFiles: true},
		{name: "action input default", site: ExpressionActionInputDefault, contexts: "github open_actions strategy matrix job runner", hashFiles: true},
		{name: "composite step", site: ExpressionCompositeStep, contexts: "github open_actions strategy matrix job runner env inputs steps", hashFiles: true},
		{name: "composite condition", site: ExpressionCompositeCondition, contexts: "github open_actions strategy matrix job runner env inputs steps", status: true, hashFiles: true},
		{name: "composite output", site: ExpressionCompositeOutput, contexts: "github open_actions strategy matrix job runner env inputs steps"},
	}
	allContexts := strings.Fields("github open_actions needs strategy matrix job runner env vars secrets steps inputs jobs")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			availability := ExpressionAvailability(test.site)
			allowed := map[string]bool{}
			for _, name := range strings.Fields(test.contexts) {
				allowed[name] = true
			}
			for _, name := range allContexts {
				program, err := expression.Parse("${{ " + name + ".value }}")
				if err != nil {
					t.Fatal(err)
				}
				err = program.Validate(availability)
				if allowed[name] && err != nil {
					t.Errorf("context %q is unavailable: %v", name, err)
				}
				if !allowed[name] && err == nil {
					t.Errorf("context %q is available", name)
				}
			}
			assertFunctionAvailability(t, availability, "success()", test.status)
			assertFunctionAvailability(t, availability, "hashFiles('**')", test.hashFiles)
		})
	}
}

func assertFunctionAvailability(t *testing.T, availability expression.Availability, function string, allowed bool) {
	t.Helper()
	program, err := expression.Parse("${{ " + function + " }}")
	if err != nil {
		t.Fatal(err)
	}
	err = program.Validate(availability)
	if allowed && err != nil {
		t.Errorf("%s is unavailable: %v", function, err)
	}
	if !allowed && err == nil {
		t.Errorf("%s is available", function)
	}
}

func TestJobPlanningUsesNeeds(t *testing.T) {
	tests := []struct {
		name string
		job  Job
		want bool
	}{
		{name: "name", job: Job{Name: "${{ needs.prepare.result }}"}, want: true},
		{name: "runner label", job: Job{RunsOn: StringList{"${{ needs.prepare.outputs.runner }}"}}, want: true},
		{name: "timeout", job: Job{TimeoutMinutes: JobTimeout{expression: "${{ needs.prepare.outputs.timeout }}"}}, want: true},
		{name: "matrix", job: Job{Strategy: Strategy{Matrix: MatrixDefinition{Expression: "${{ needs.prepare.outputs.matrix }}"}}}, want: true},
		{name: "job environment", job: Job{Env: map[string]any{"VALUE": "${{ needs.prepare.result }}"}}},
		{name: "job concurrency", job: Job{Concurrency: Concurrency{Group: "${{ needs.prepare.result }}"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := JobPlanningUsesNeeds(test.job); got != test.want {
				t.Errorf("JobPlanningUsesNeeds() = %t, want %t", got, test.want)
			}
		})
	}
}
