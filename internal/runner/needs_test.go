package runner

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	workflowexpression "github.com/kelos-dev/open-actions/internal/expression"
)

func TestNeedsContextRoundTrip(t *testing.T) {
	needs := Needs{
		"build": {Result: "success", Outputs: map[string]string{"artifact": "ready"}},
		"lint":  {Result: "failure", Outputs: map[string]string{}},
	}
	data, err := EncodeNeedsContext(needs)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeNeedsContext(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["build"].Result != "success" || decoded["build"].Outputs["artifact"] != "ready" || decoded["lint"].Result != "failure" {
		t.Fatalf("needs = %#v", decoded)
	}

	path := filepath.Join(t.TempDir(), "needs.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := testPlan()
	if err := LoadNeedsContext(plan, path); err != nil {
		t.Fatal(err)
	}
	if plan.Needs["build"].Outputs["artifact"] != "ready" {
		t.Fatalf("plan needs = %#v", plan.Needs)
	}
}

func TestDecodeNeedsContextRejectsInvalidDocuments(t *testing.T) {
	tests := map[string]string{
		"empty dependencies": `{"version":1,"needs":{}}`,
		"unknown field":      `{"version":1,"needs":{"build":{"result":"success","outputs":{},"extra":true}}}`,
		"invalid result":     `{"version":1,"needs":{"build":{"result":"neutral","outputs":{}}}}`,
		"invalid output":     `{"version":1,"needs":{"build":{"result":"success","outputs":{"invalid.name":"value"}}}}`,
		"trailing value":     `{"version":1,"needs":{"build":{"result":"success","outputs":{}}}} {}`,
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeNeedsContext([]byte(data)); err == nil {
				t.Fatal("DecodeNeedsContext() accepted an invalid document")
			}
		})
	}
}

func TestEncodeNeedsContextEnforcesSizeLimit(t *testing.T) {
	needs := make(Needs, maxNeeds)
	for index := 0; index < maxNeeds; index++ {
		needs["job-"+strconv.Itoa(index)] = Need{
			Result:  "success",
			Outputs: map[string]string{"value": strings.Repeat("x", 1000)},
		}
	}
	_, err := EncodeNeedsContext(needs)
	if err == nil || !strings.Contains(err.Error(), "needs context exceeds") {
		t.Fatalf("EncodeNeedsContext() error = %v, want size limit", err)
	}
}

func TestNeedsContextAllowsAggregatedMatrixOutputs(t *testing.T) {
	outputs := make(map[string]string, MaxJobOutputs+1)
	for index := 0; index <= MaxJobOutputs; index++ {
		outputs["output_"+strconv.Itoa(index)] = "value"
	}
	data, err := EncodeNeedsContext(Needs{"build": {Result: "success", Outputs: outputs}})
	if err != nil {
		t.Fatal(err)
	}
	needs, err := DecodeNeedsContext(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(needs["build"].Outputs) != MaxJobOutputs+1 {
		t.Fatalf("outputs = %d", len(needs["build"].Outputs))
	}
}

func TestNeedsContextSupportsWorkflowStepExpressions(t *testing.T) {
	plan := testPlan()
	plan.Needs = Needs{"build": {
		Result: "success",
		Outputs: map[string]string{
			"artifact":  "release.tar.gz",
			"directory": "dist",
		},
	}}
	state := &executionState{
		plan:        plan,
		environment: []string{"GITHUB_WORKSPACE=/workspace"},
		stepOutputs: map[string]map[string]any{},
	}
	raw := Step{
		Name:             "Publish ${{ needs.build.result }}",
		Run:              "upload ${{ needs.build.outputs.artifact }}",
		WorkingDirectory: "${{ needs.build.outputs.directory }}",
		With:             map[string]string{"artifact": "${{ needs.build.outputs.artifact }}"},
		Env:              map[string]string{"RESULT": "${{ needs.build.result }}"},
	}
	environment, err := resolveWorkflowStepEnvironment(raw, state)
	if err != nil {
		t.Fatal(err)
	}
	step, err := resolveWorkflowStep(raw, environment, state)
	if err != nil {
		t.Fatal(err)
	}
	if step.Name != "Publish success" || step.Run != "upload release.tar.gz" || step.WorkingDirectory != "dist" || step.With["artifact"] != "release.tar.gz" || step.Env["RESULT"] != "success" {
		t.Fatalf("resolved step = %#v", step)
	}

	jobEnvironment, err := resolveJobEnvironment(map[string]string{"ARTIFACT": "${{ needs.build.outputs.artifact }}"}, plan, nil, "token", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if jobEnvironment["ARTIFACT"] != "release.tar.gz" {
		t.Fatalf("job environment = %#v", jobEnvironment)
	}
	plan.Outputs = map[string]string{"upstream": "${{ needs.build.outputs.artifact }}"}
	outputs, err := testExecutor(t, io.Discard, io.Discard).resolveJobOutputs(state)
	if err != nil {
		t.Fatal(err)
	}
	if outputs["upstream"] != "release.tar.gz" {
		t.Fatalf("job outputs = %#v", outputs)
	}

	missing, err := resolveExpressionString("${{ needs.build.outputs.missing }}", workflowExpressionContext(state, nil, runnerStepAvailability, nil))
	if err != nil {
		t.Fatal(err)
	}
	if missing != "" {
		t.Fatalf("missing output = %q", missing)
	}
	unrelatedPlan := testPlan()
	unrelatedState := &executionState{plan: unrelatedPlan, stepOutputs: map[string]map[string]any{}}
	unrelated, err := resolveExpressionString("${{ needs.build.result }}", workflowExpressionContext(unrelatedState, nil, runnerStepAvailability, nil))
	if err != nil {
		t.Fatal(err)
	}
	if unrelated != "" {
		t.Fatalf("undeclared dependency result = %q", unrelated)
	}
}

func TestNeedsContextSupportsEveryTerminalResultInStepConditions(t *testing.T) {
	for _, result := range []string{"success", "failure", "skipped", "cancelled"} {
		t.Run(result, func(t *testing.T) {
			plan := testPlan()
			plan.Needs = Needs{"build": {Result: result, Outputs: map[string]string{}}}
			state := &executionState{plan: plan, stepOutputs: map[string]map[string]any{}}
			matched, err := workflowStepCondition("needs.build.result == '"+result+"'", nil, workflowexpression.Status{Success: true}, state)
			if err != nil {
				t.Fatal(err)
			}
			if !matched {
				t.Fatalf("condition did not match %q", result)
			}
		})
	}
}

func TestPlanJSONDoesNotEmbedNeedsContext(t *testing.T) {
	plan := testPlan()
	plan.Needs = Needs{"build": {Result: "success"}}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"needs"`) {
		t.Fatalf("plan contains separately persisted needs context: %s", data)
	}
}
