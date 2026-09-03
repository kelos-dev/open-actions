package apischema

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsvalidation "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	celvalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	structurallisttype "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	customresourcevalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

func TestWorkflowRunAcceptsGitPathCharacters(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	validateSample(t, crd, "actions_v1alpha1_workflowrun-git-paths.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun-git-paths.yaml")
	github := object["spec"].(map[string]any)["source"].(map[string]any)["github"].(map[string]any)
	github["revision"].(map[string]any)["ref"] = "refs/heads/release./candidate+api@review"
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid Git ref was rejected: %v", errs.ToAggregate())
	}
}

func TestProjectAcceptsValueSources(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_projects.yaml")
	validateSample(t, crd, "actions_v1alpha1_project-values.yaml")
}

func TestWorkflowRunAcceptsTimedOutCheckConclusion(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(object)
	object["status"] = map[string]any{
		"source": map[string]any{
			"github": map[string]any{
				"checkRun": map[string]any{"id": int64(1), "status": "completed", "conclusion": "timed_out"},
			},
		},
	}
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("timed_out check conclusion was rejected: %v", errs.ToAggregate())
	}
}

func TestWorkflowRunTTLIsMutable(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	original := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	updated := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(original)
	normalizeWorkflowRunCELIntegers(updated)
	updated["spec"].(map[string]any)["ttlSecondsAfterFinished"] = int64(3600)
	if errs := validateObject(t, crd, updated, original); len(errs) > 0 {
		t.Fatalf("WorkflowRun TTL update was rejected: %v", errs.ToAggregate())
	}

	retained := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(retained)
	delete(retained["spec"].(map[string]any), "ttlSecondsAfterFinished")
	if errs := validateObject(t, crd, retained, updated); len(errs) > 0 {
		t.Fatalf("WorkflowRun TTL removal was rejected: %v", errs.ToAggregate())
	}

	changedExecution := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(changedExecution)
	changedExecution["spec"].(map[string]any)["workflowPath"] = ".open-actions/workflows/deploy.yaml"
	if errs := validateObject(t, crd, changedExecution, original); len(errs) == 0 {
		t.Fatal("WorkflowRun execution update passed CEL validation")
	}
}

func TestWorkflowRunCancellationRequestIsMonotonic(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	original := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	requested := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(original)
	normalizeWorkflowRunCELIntegers(requested)
	requested["spec"].(map[string]any)["cancelRequested"] = true
	if errs := validateObject(t, crd, requested, original); len(errs) > 0 {
		t.Fatalf("WorkflowRun cancellation request was rejected: %v", errs.ToAggregate())
	}

	cleared := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(cleared)
	if errs := validateObject(t, crd, cleared, requested); len(errs) == 0 {
		t.Fatal("WorkflowRun cancellation request was cleared")
	}
}

func TestWorkflowRunForkPullRequestApprovalContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	newObject := func() map[string]any {
		object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
		normalizeWorkflowRunCELIntegers(object)
		workflowRunGitHub(object)["event"].(map[string]any)["pullRequest"] = pullRequestEventMetadata()
		object["spec"].(map[string]any)["forkPullRequest"] = map[string]any{
			"requireApproval": true,
			"approved":        false,
			"sendWriteTokens": false,
			"sendSecrets":     false,
		}
		return object
	}
	original := newObject()
	if errs := validateObject(t, crd, original, nil); len(errs) > 0 {
		t.Fatalf("valid fork pull request policy was rejected: %v", errs.ToAggregate())
	}
	approved := newObject()
	approved["spec"].(map[string]any)["forkPullRequest"].(map[string]any)["approved"] = true
	if errs := validateObject(t, crd, approved, original); len(errs) > 0 {
		t.Fatalf("fork pull request approval was rejected: %v", errs.ToAggregate())
	}
	if errs := validateObject(t, crd, original, approved); len(errs) == 0 {
		t.Fatal("fork pull request approval was revoked")
	}
	changedPolicy := newObject()
	changedPolicy["spec"].(map[string]any)["forkPullRequest"].(map[string]any)["sendSecrets"] = true
	if errs := validateObject(t, crd, changedPolicy, original); len(errs) == 0 {
		t.Fatal("fork pull request policy update passed validation")
	}
	withoutApproval := newObject()
	delete(withoutApproval["spec"].(map[string]any), "forkPullRequest")
	if errs := validateObject(t, crd, withoutApproval, original); len(errs) == 0 {
		t.Fatal("fork pull request policy removal passed validation")
	}
	wrongEvent := newObject()
	setBranchEvent(workflowRunGitHub(wrongEvent), "push", "")
	if errs := validateObject(t, crd, wrongEvent, nil); len(errs) == 0 {
		t.Fatal("fork pull request policy on a push event passed validation")
	}
	unnecessaryApproval := newObject()
	policy := unnecessaryApproval["spec"].(map[string]any)["forkPullRequest"].(map[string]any)
	policy["requireApproval"] = false
	policy["approved"] = true
	if errs := validateObject(t, crd, unnecessaryApproval, nil); len(errs) == 0 {
		t.Fatal("approval without an approval requirement passed validation")
	}
}

func TestWorkflowJobConcurrencyCancellationUnion(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowjobs.yaml")
	for _, test := range []struct {
		name                string
		cancelInProgress    map[string]any
		wantValidationError bool
	}{
		{name: "literal", cancelInProgress: map[string]any{"value": false}},
		{name: "expression", cancelInProgress: map[string]any{"expression": "${{ matrix.environment != 'production' }}"}},
		{name: "neither", cancelInProgress: map[string]any{}, wantValidationError: true},
		{name: "both", cancelInProgress: map[string]any{"value": true, "expression": "${{ true }}"}, wantValidationError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			object := loadWorkflowJobSample(t)
			object["spec"].(map[string]any)["concurrency"] = map[string]any{
				"group":            "deploy",
				"cancelInProgress": test.cancelInProgress,
			}
			errs := validateObject(t, crd, object, nil)
			if test.wantValidationError && len(errs) == 0 {
				t.Fatal("invalid concurrency cancellation passed CEL validation")
			}
			if !test.wantValidationError && len(errs) > 0 {
				t.Fatalf("valid concurrency cancellation was rejected: %v", errs.ToAggregate())
			}
		})
	}
}

func TestWorkflowRunRerunContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	original := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(original)
	original["spec"].(map[string]any)["rerun"] = map[string]any{
		"originalRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
		"previousRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
		"attempt":        int64(2),
		"requestID":      "delivery-123",
		"jobIDs":         []any{"unit-matrix-2", "integration"},
	}
	if errs := validateObject(t, crd, original, nil); len(errs) > 0 {
		t.Fatalf("valid WorkflowRun rerun was rejected: %v", errs.ToAggregate())
	}

	updated := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(updated)
	updated["spec"].(map[string]any)["rerun"] = map[string]any{
		"originalRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
		"previousRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
		"attempt":        int64(2),
		"jobIDs":         []any{"integration"},
	}
	if errs := validateObject(t, crd, updated, original); len(errs) == 0 {
		t.Fatal("WorkflowRun rerun update passed CEL validation")
	}

	invalidAttempt := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	normalizeWorkflowRunCELIntegers(invalidAttempt)
	invalidAttempt["spec"].(map[string]any)["rerun"] = map[string]any{
		"originalRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
		"previousRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
		"attempt":        int64(1),
	}
	if errs := validateObject(t, crd, invalidAttempt, nil); len(errs) == 0 {
		t.Fatal("WorkflowRun rerun attempt 1 passed schema validation")
	}

	for _, test := range []struct {
		name string
		ref  map[string]any
	}{
		{name: "oversized reference name", ref: map[string]any{"name": strings.Repeat("a", 254), "uid": "original-uid"}},
		{name: "malformed reference UID", ref: map[string]any{"name": "ci-original", "uid": "invalid/uid"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalidReference := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
			normalizeWorkflowRunCELIntegers(invalidReference)
			invalidReference["spec"].(map[string]any)["rerun"] = map[string]any{
				"originalRunRef": test.ref,
				"previousRunRef": map[string]any{"name": "ci-original", "uid": "original-uid"},
				"attempt":        int64(2),
			}
			if errs := validateObject(t, crd, invalidReference, nil); len(errs) == 0 {
				t.Fatal("invalid WorkflowRun reference passed schema validation")
			}
		})
	}
}

func TestWorkflowRunIdentityContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	object["status"] = map[string]any{"identity": map[string]any{
		"id": int64(101), "number": int64(7), "attempt": int64(2),
		"url": "https://actions.example/runs/default/ci-attempt-2",
	}}
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid WorkflowRun identity was rejected: %v", errs.ToAggregate())
	}

	object["status"].(map[string]any)["identity"].(map[string]any)["id"] = int64(0)
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("zero WorkflowRun identity passed schema validation")
	}
}

func normalizeWorkflowRunCELIntegers(object map[string]any) {
	repository := workflowRunGitHub(object)["repository"].(map[string]any)
	if id, ok := repository["id"].(float64); ok {
		repository["id"] = int64(id)
	}
}

func TestWorkflowRunAcceptsGitHubEventContracts(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	validateSample(t, crd, "actions_v1alpha1_workflowrun-schedule.yaml")
	validateSample(t, crd, "actions_v1alpha1_workflowrun-dispatch.yaml")
	for _, tt := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "closed merged pull request",
			mutate: func(github map[string]any) {
				github["event"].(map[string]any)["action"] = "closed"
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "baseSHA")
				delete(revision, "mergeBaseSHA")
			},
		},
		{
			name: "closed unmerged pull request",
			mutate: func(github map[string]any) {
				github["event"].(map[string]any)["action"] = "closed"
				revision := github["revision"].(map[string]any)
				delete(revision, "baseSHA")
				delete(revision, "mergeBaseSHA")
			},
		},
		{
			name: "merge group",
			mutate: func(github map[string]any) {
				event := github["event"].(map[string]any)
				event["name"] = "merge_group"
				event["action"] = "checks_requested"
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/gh-readonly-queue/main/pr-42"
				delete(revision, "headSHA")
				delete(revision, "headRef")
				delete(revision, "baseSHA")
				delete(revision, "mergeBaseSHA")
			},
		},
		{
			name: "workflow run",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "workflow_run", "completed")
				github["event"].(map[string]any)["workflowRun"] = map[string]any{"conclusion": "success", "headSHA": strings.Repeat("a", 40)}
			},
		},
		{
			name: "workflow dispatch with inputs",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "workflow_dispatch", "")
				github["event"].(map[string]any)["inputs"] = map[string]any{"namespace": "default"}
			},
		},
		{
			name: "issues",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "issues", "opened")
				github["event"].(map[string]any)["issue"] = map[string]any{"number": int64(42), "body": "/kind bug"}
			},
		},
		{
			name: "pull request target",
			mutate: func(github map[string]any) {
				event := github["event"].(map[string]any)
				event["name"] = "pull_request_target"
				event["pullRequest"] = pullRequestEventMetadata()
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "headSHA")
				delete(revision, "headRef")
				delete(revision, "baseRef")
				delete(revision, "baseSHA")
				delete(revision, "mergeBaseSHA")
			},
		},
		{
			name: "issue comment",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "issue_comment", "created")
				event := github["event"].(map[string]any)
				event["issue"] = map[string]any{"number": int64(42), "body": "Issue body"}
				event["comment"] = map[string]any{"body": "/kind bug"}
			},
		},
		{
			name: "pull request review comment",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "pull_request_review_comment", "created")
				event := github["event"].(map[string]any)
				event["pullRequest"] = pullRequestEventMetadata()
				event["comment"] = map[string]any{"body": "/priority important-soon"}
			},
		},
		{
			name: "pull request review",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "pull_request_review", "submitted")
				event := github["event"].(map[string]any)
				event["pullRequest"] = pullRequestEventMetadata()
				event["review"] = map[string]any{"body": "/kind api"}
			},
		},
		{
			name: "schedule",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "schedule", "")
				github["event"].(map[string]any)["schedule"] = "0 6 * * *"
			},
		},
		{
			name: "release",
			mutate: func(github map[string]any) {
				event := github["event"].(map[string]any)
				event["name"] = "release"
				event["action"] = "published"
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/tags/v1.0.0"
				delete(revision, "headSHA")
				delete(revision, "headRef")
				delete(revision, "baseRef")
				delete(revision, "baseSHA")
				delete(revision, "mergeBaseSHA")
			},
		},
		{
			name: "workflow call with inputs",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "workflow_call", "")
				github["event"].(map[string]any)["inputs"] = map[string]any{"enabled": "true"}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
			tt.mutate(workflowRunGitHub(object))
			if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
				t.Fatalf("valid GitHub event contract was rejected: %v", errs.ToAggregate())
			}
		})
	}
}

func TestWorkflowRunValidatesPullRequestTargetMetadata(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	github := workflowRunGitHub(object)
	event := github["event"].(map[string]any)
	event["name"] = "pull_request_target"
	revision := github["revision"].(map[string]any)
	revision["ref"] = "refs/heads/main"
	delete(revision, "headSHA")
	delete(revision, "headRef")
	delete(revision, "baseRef")
	delete(revision, "baseSHA")
	delete(revision, "mergeBaseSHA")
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("pull_request_target without metadata passed validation")
	}
	event["pullRequest"] = map[string]any{
		"number": int64(42), "body": "", "htmlURL": "github.com/contributor/example/pull/42",
		"headSHA": strings.Repeat("b", 40), "headRef": "feature", "baseRef": "main",
		"headRepository": map[string]any{"id": int64(2), "owner": "contributor", "name": "example"},
	}
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("pull_request_target with an invalid HTML URL passed validation")
	}
	event["pullRequest"] = pullRequestEventMetadata()
	revision["ref"] = "refs/heads/release"
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("pull_request_target with a revision outside its base branch passed validation")
	}
}

func TestWorkflowRunRequiresGitHubEventMetadata(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	for _, tt := range []struct {
		name       string
		eventName  string
		action     string
		missingKey string
		metadata   map[string]any
	}{
		{name: "workflow run", eventName: "workflow_run", action: "completed", missingKey: "workflowRun", metadata: map[string]any{"workflowRun": map[string]any{"conclusion": "success", "headSHA": strings.Repeat("a", 40)}}},
		{name: "issues", eventName: "issues", action: "opened", missingKey: "issue", metadata: map[string]any{"issue": map[string]any{"number": int64(42), "body": "Issue body"}}},
		{name: "issue comment", eventName: "issue_comment", action: "created", missingKey: "comment", metadata: map[string]any{"issue": map[string]any{"number": int64(42), "body": "Issue body"}, "comment": map[string]any{"body": "Comment body"}}},
		{name: "review comment", eventName: "pull_request_review_comment", action: "created", missingKey: "comment", metadata: map[string]any{"pullRequest": pullRequestEventMetadata(), "comment": map[string]any{"body": "Comment body"}}},
		{name: "review", eventName: "pull_request_review", action: "submitted", missingKey: "review", metadata: map[string]any{"pullRequest": pullRequestEventMetadata(), "review": map[string]any{"body": "Review body"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
			github := workflowRunGitHub(object)
			event := github["event"].(map[string]any)
			setBranchEvent(github, tt.eventName, tt.action)
			for key, value := range tt.metadata {
				event[key] = value
			}
			if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
				t.Fatalf("valid event metadata was rejected: %v", errs.ToAggregate())
			}
			delete(event, tt.missingKey)
			if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
				t.Fatalf("%s event without %s passed validation", tt.eventName, tt.missingKey)
			}
		})
	}
}

func TestWorkflowRunRejectsMismatchedGitHubEventMetadata(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	for _, tt := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "completed workflow run without conclusion",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "workflow_run", "completed")
				github["event"].(map[string]any)["workflowRun"] = map[string]any{"headSHA": strings.Repeat("a", 40)}
			},
		},
		{
			name: "review without pull request",
			mutate: func(github map[string]any) {
				setBranchEvent(github, "pull_request_review", "submitted")
				github["event"].(map[string]any)["review"] = map[string]any{"body": "Review body"}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
			tt.mutate(workflowRunGitHub(object))
			if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
				t.Fatal("invalid GitHub event metadata passed validation")
			}
		})
	}
}

func TestWorkflowRunValidatesPullRequestMetadataConsistency(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	for _, tt := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "head ref", mutate: func(metadata map[string]any) { metadata["headRef"] = "other-branch" }},
		{name: "base ref", mutate: func(metadata map[string]any) { metadata["baseRef"] = "other-branch" }},
		{name: "head SHA", mutate: func(metadata map[string]any) { metadata["headSHA"] = strings.Repeat("a", 40) }},
		{name: "number", mutate: func(metadata map[string]any) { metadata["number"] = int64(43) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
			event := workflowRunGitHub(object)["event"].(map[string]any)
			metadata := pullRequestEventMetadata()
			event["pullRequest"] = metadata
			if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
				t.Fatalf("consistent pull request metadata was rejected: %v", errs.ToAggregate())
			}
			tt.mutate(metadata)
			if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
				t.Fatal("inconsistent pull request metadata passed validation")
			}
		})
	}
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	github := workflowRunGitHub(object)
	github["event"].(map[string]any)["pullRequest"] = pullRequestEventMetadata()
	delete(github["revision"].(map[string]any), "headSHA")
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("pull request metadata without revision.headSHA passed validation")
	}
	for _, fieldName := range []string{"baseSHA", "mergeBaseSHA"} {
		object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
		delete(workflowRunGitHub(object)["revision"].(map[string]any), fieldName)
		if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
			t.Fatalf("pull request with an unpaired revision.%s passed validation", fieldName)
		}
	}
	object = loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	delete(workflowRunGitHub(object)["revision"].(map[string]any), "headSHA")
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("pull request integration revision without revision.headSHA passed validation")
	}
	object = loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	workflowRunGitHub(object)["event"].(map[string]any)["action"] = "closed"
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("closed pull request with integration inputs passed validation")
	}
}

func TestWorkflowRunBoundsGitHubEventMetadata(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	github := workflowRunGitHub(object)
	setBranchEvent(github, "issues", "opened")
	issue := map[string]any{"number": int64(42), "body": strings.Repeat("x", 48_000)}
	github["event"].(map[string]any)["issue"] = issue
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("maximum issue body was rejected: %v", errs.ToAggregate())
	}
	issue["body"] = strings.Repeat("x", 48_001)
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("oversized issue body passed validation")
	}
}

func pullRequestEventMetadata() map[string]any {
	return map[string]any{
		"number": int64(42), "body": "Pull request body", "htmlURL": "https://github.com/contributor/example/pull/42",
		"headSHA": "fedcba9876543210fedcba9876543210fedcba98", "headRef": "feature-branch", "baseRef": "main",
		"headRepository": map[string]any{"id": int64(2), "owner": "contributor", "name": "example"},
	}
}

func TestWorkflowRunInputPayloadBoundary(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun-dispatch.yaml")
	inputs := workflowRunGitHub(object)["event"].(map[string]any)["inputs"].(map[string]any)
	inputs["a"] = strings.Repeat("x", 65_534)
	delete(inputs, "environment")
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("input payload at the maximum was rejected: %v", errs.ToAggregate())
	}

	inputs["a"] = strings.Repeat("x", 65_535)
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("input payload above the maximum passed validation")
	}
}

func setBranchEvent(github map[string]any, name, action string) {
	event := github["event"].(map[string]any)
	event["name"] = name
	if name == "workflow_dispatch" || name == "schedule" || name == "workflow_call" {
		delete(event, "deliveryID")
	}
	if action == "" {
		delete(event, "action")
	} else {
		event["action"] = action
	}
	revision := github["revision"].(map[string]any)
	revision["ref"] = "refs/heads/main"
	delete(revision, "headSHA")
	delete(revision, "headRef")
	delete(revision, "baseRef")
	delete(revision, "baseSHA")
	delete(revision, "mergeBaseSHA")
}

func TestWorkflowRunAcceptsAtBranchName(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	workflowRunGitHub(object)["revision"].(map[string]any)["headRef"] = "@"
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid @ branch name was rejected: %v", errs.ToAggregate())
	}
}

func TestWorkflowRunAcceptsPullRequestWithoutIntegrationInputs(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	for _, tt := range []struct {
		name       string
		removeHead bool
	}{
		{name: "with head SHA"},
		{name: "without head SHA", removeHead: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
			revision := workflowRunGitHub(object)["revision"].(map[string]any)
			if tt.removeHead {
				delete(revision, "headSHA")
			}
			delete(revision, "baseSHA")
			delete(revision, "mergeBaseSHA")
			if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
				t.Fatalf("pull request without integration inputs was rejected: %v", errs.ToAggregate())
			}
		})
	}
}

func TestWorkflowRunAcceptsManagedUserOwner(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	workflowRunGitHub(object)["repository"].(map[string]any)["owner"] = "octocat_enterprise"
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("managed user repository owner was rejected: %v", errs.ToAggregate())
	}
}

func TestWorkflowRunAcceptsGitHubCheckRunStatusContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	object["status"] = map[string]any{"source": map[string]any{"github": map[string]any{"checkRun": map[string]any{
		"id": int64(17), "status": "completed", "conclusion": "success", "reportDigest": strings.Repeat("a", 64),
	}}}}
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid GitHub check-run status was rejected: %v", errs.ToAggregate())
	}
	delete(object["status"].(map[string]any)["source"].(map[string]any)["github"].(map[string]any)["checkRun"].(map[string]any), "conclusion")
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("completed GitHub check-run status without a conclusion was accepted")
	}
	checkRun := object["status"].(map[string]any)["source"].(map[string]any)["github"].(map[string]any)["checkRun"].(map[string]any)
	checkRun["conclusion"] = "success"
	checkRun["reportDigest"] = "invalid"
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("GitHub check-run status with an invalid report digest was accepted")
	}
}

func TestRunnerAcceptsQualifiedResourceNames(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	object := loadSample(t, "actions_v1alpha1_runner.yaml")
	resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
	resources["requests"].(map[string]any)["example.com/accelerator"] = "1"
	resources["limits"].(map[string]any)["example.com/accelerator"] = "1"
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("qualified resource name was rejected: %v", errs.ToAggregate())
	}
}

func TestRunnerAcceptsDockerExecution(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	validateSample(t, crd, "actions_v1alpha1_docker_runner.yaml")
}

func TestRunnerPodResourcesAcceptOnlySupportedResources(t *testing.T) {
	tests := []struct {
		name       string
		requests   map[string]any
		limits     map[string]any
		wantErrors bool
	}{
		{name: "CPU", requests: map[string]any{"cpu": "1"}, limits: map[string]any{"cpu": "2"}},
		{name: "integer CPU", requests: map[string]any{"cpu": int64(1)}, limits: map[string]any{"cpu": int64(2)}},
		{name: "memory", requests: map[string]any{"memory": "1Gi"}, limits: map[string]any{"memory": "2Gi"}},
		{
			name:     "huge pages",
			requests: map[string]any{"cpu": "1", "hugepages-2Mi": "1Gi"},
			limits:   map[string]any{"cpu": "2", "hugepages-2Mi": "1Gi"},
		},
		{name: "ephemeral storage", requests: map[string]any{"ephemeral-storage": "1Gi"}, limits: map[string]any{"ephemeral-storage": "2Gi"}, wantErrors: true},
		{name: "extended resource", requests: map[string]any{"example.com/accelerator": "1"}, limits: map[string]any{"example.com/accelerator": "1"}, wantErrors: true},
		{name: "request above limit", requests: map[string]any{"cpu": "3"}, limits: map[string]any{"cpu": "2"}, wantErrors: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
			object := loadSample(t, "actions_v1alpha1_runner.yaml")
			execution := object["spec"].(map[string]any)["execution"].(map[string]any)
			execution["resources"] = map[string]any{
				"requests": test.requests,
				"limits":   test.limits,
			}
			errs := validateObject(t, crd, object, nil)
			if test.wantErrors && len(errs) == 0 {
				t.Fatal("invalid Pod-level resources passed validation")
			}
			if !test.wantErrors && len(errs) > 0 {
				t.Fatalf("valid Pod-level resources were rejected: %v", errs.ToAggregate())
			}
		})
	}
}

func TestRunnerPodResourcesMustNotBeEmpty(t *testing.T) {
	tests := []struct {
		name      string
		resources map[string]any
		wantError bool
	}{
		{name: "empty resources", resources: map[string]any{}, wantError: true},
		{name: "empty requests", resources: map[string]any{"requests": map[string]any{}}, wantError: true},
		{name: "empty limits", resources: map[string]any{"limits": map[string]any{}}, wantError: true},
		{name: "empty requests with a limit", resources: map[string]any{"requests": map[string]any{}, "limits": map[string]any{"cpu": "1"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
			object := loadSample(t, "actions_v1alpha1_runner.yaml")
			execution := object["spec"].(map[string]any)["execution"].(map[string]any)
			execution["resources"] = test.resources
			errs := validateObject(t, crd, object, nil)
			if test.wantError && len(errs) == 0 {
				t.Fatal("empty Pod-level resources passed validation")
			}
			if !test.wantError && len(errs) > 0 {
				t.Fatalf("non-empty Pod-level resources were rejected: %v", errs.ToAggregate())
			}
		})
	}
}

func TestRunnerSetTemplateProjectRefIsImmutable(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runnersets.yaml")
	original := loadSample(t, "actions_v1alpha1_runnerset.yaml")
	updated := loadSample(t, "actions_v1alpha1_runnerset.yaml")
	templateSpec := updated["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	templateSpec["projectRef"].(map[string]any)["name"] = "other"
	if errs := validateObject(t, crd, updated, original); len(errs) == 0 {
		t.Fatal("RunnerSet template projectRef update passed CEL validation")
	}
}

func TestRunnerTerminationGracePeriodContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	object := loadSample(t, "actions_v1alpha1_runner.yaml")
	execution := object["spec"].(map[string]any)["execution"].(map[string]any)
	execution["terminationGracePeriodSeconds"] = int64(0)
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("zero termination grace period was rejected: %v", errs.ToAggregate())
	}

	execution["terminationGracePeriodSeconds"] = int64(-1)
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("negative termination grace period passed validation")
	}
}

func TestRunnerImagePullPolicyContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	for _, policy := range []string{"Always", "Never", "IfNotPresent"} {
		t.Run(policy, func(t *testing.T) {
			object := loadSample(t, "actions_v1alpha1_runner.yaml")
			runner := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)
			runner["imagePullPolicy"] = policy
			if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
				t.Fatalf("image pull policy %q was rejected: %v", policy, errs.ToAggregate())
			}
		})
	}

	object := loadSample(t, "actions_v1alpha1_runner.yaml")
	runner := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)
	runner["imagePullPolicy"] = "Sometimes"
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("invalid image pull policy passed validation")
	}
}

func TestRunnerEnvironmentContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	object := loadSample(t, "actions_v1alpha1_runner.yaml")
	runner := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)
	runner["env"] = []any{
		map[string]any{"name": "CACHE_URL", "value": "https://cache.example"},
		map[string]any{"name": "CACHE_TOKEN", "valueFrom": map[string]any{
			"secretKeyRef": map[string]any{"name": "runner-environment", "key": "cache-token"},
		}},
	}
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid runner environment was rejected: %v", errs.ToAggregate())
	}

	environment := make([]any, 101)
	for index := range environment {
		environment[index] = map[string]any{"name": fmt.Sprintf("VARIABLE_%d", index), "value": "value"}
	}
	runner["env"] = environment
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("more than 100 runner environment variables passed validation")
	}

	runner["env"] = []any{
		map[string]any{"name": "CACHE_URL", "value": "one"},
		map[string]any{"name": "CACHE_URL", "value": "two"},
	}
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("duplicate runner environment variables passed validation")
	}
}

func TestRunnerImagePullSecretsContract(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	object := loadSample(t, "actions_v1alpha1_runner.yaml")
	execution := object["spec"].(map[string]any)["execution"].(map[string]any)
	execution["imagePullSecrets"] = []any{map[string]any{"name": "registry-credentials"}}
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid image pull secret was rejected: %v", errs.ToAggregate())
	}

	execution["imagePullSecrets"] = []any{map[string]any{"name": "invalid/name"}}
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("invalid image pull secret passed validation")
	}

	imagePullSecrets := make([]any, 33)
	for index := range imagePullSecrets {
		imagePullSecrets[index] = map[string]any{"name": fmt.Sprintf("registry-credentials-%d", index)}
	}
	execution["imagePullSecrets"] = imagePullSecrets
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("more than 32 image pull secrets passed validation")
	}

	execution["imagePullSecrets"] = []any{
		map[string]any{"name": "registry-credentials"},
		map[string]any{"name": "registry-credentials"},
	}
	if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
		t.Fatal("duplicate image pull secrets passed validation")
	}
}

func TestCRDConventions(t *testing.T) {
	tests := []struct {
		file                    string
		sample                  string
		kind                    string
		requiredSpec            []string
		hasSpecUpdateValidation bool
	}{
		{
			file:         "actions.kelos.dev_projects.yaml",
			sample:       "actions_v1alpha1_project.yaml",
			kind:         "Project",
			requiredSpec: []string{"source"},
		},
		{
			file:         "actions.kelos.dev_runners.yaml",
			sample:       "actions_v1alpha1_runner.yaml",
			kind:         "Runner",
			requiredSpec: []string{"execution", "projectRef", "labels"},
		},
		{
			file:         "actions.kelos.dev_runnersets.yaml",
			sample:       "actions_v1alpha1_runnerset.yaml",
			kind:         "RunnerSet",
			requiredSpec: []string{"template"},
		},
		{
			file:                    "actions.kelos.dev_workflowjobs.yaml",
			sample:                  "actions_v1alpha1_workflowjob.yaml",
			kind:                    "WorkflowJob",
			requiredSpec:            []string{"jobID", "runsOn", "workflowRunRef"},
			hasSpecUpdateValidation: true,
		},
		{
			file:                    "actions.kelos.dev_workflowruns.yaml",
			sample:                  "actions_v1alpha1_workflowrun.yaml",
			kind:                    "WorkflowRun",
			requiredSpec:            []string{"projectRef", "source", "workflowPath"},
			hasSpecUpdateValidation: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			crd, size := loadCRD(t, tt.file)
			if size > 100_000 {
				t.Errorf("generated CRD is %d bytes; use references instead of embedding large APIs", size)
			}
			validateCRD(t, crd)
			validateSample(t, crd, tt.sample)
			if crd.Spec.Group != "actions.kelos.dev" {
				t.Fatalf("group = %q", crd.Spec.Group)
			}
			if crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
				t.Fatalf("scope = %q", crd.Spec.Scope)
			}

			version := storageVersion(t, crd)
			if version.Subresources == nil || version.Subresources.Status == nil {
				t.Fatal("status subresource is not enabled")
			}

			root := version.Schema.OpenAPIV3Schema
			spec := root.Properties["spec"]
			for _, field := range tt.requiredSpec {
				if !slices.Contains(spec.Required, field) {
					t.Errorf("spec.%s is not required", field)
				}
			}

			status := root.Properties["status"]
			if _, found := status.Properties["phase"]; found {
				t.Error("API status must use conditions rather than phase")
			}
			conditions := status.Properties["conditions"]
			if conditions.XListType == nil || *conditions.XListType != "map" {
				t.Error("status.conditions must use map list semantics")
			}
			if !slices.Contains(conditions.XListMapKeys, "type") {
				t.Error("status.conditions must use type as its list key")
			}

			if tt.hasSpecUpdateValidation && len(spec.XValidations) == 0 {
				t.Error("spec update contract has no CEL validation")
			}

			if tt.kind == "Project" {
				secrets := spec.Properties["secrets"]
				if !slices.Contains(secrets.Required, "secretRef") || len(secrets.XValidations) != 3 {
					t.Errorf("spec.secrets schema = %#v", secrets)
				}
				variables := spec.Properties["variables"]
				if !slices.Contains(variables.Required, "configMapRef") || len(variables.XValidations) != 3 {
					t.Errorf("spec.variables schema = %#v", variables)
				}
			}
			if tt.kind == "Runner" {
				execution := spec.Properties["execution"]
				if !slices.Contains(execution.Required, "runner") {
					t.Error("spec.execution.runner is not required")
				}
				terminationGracePeriod := execution.Properties["terminationGracePeriodSeconds"]
				if slices.Contains(execution.Required, "terminationGracePeriodSeconds") {
					t.Error("spec.execution.terminationGracePeriodSeconds is required")
				}
				if terminationGracePeriod.Format != "int64" || terminationGracePeriod.Minimum == nil || *terminationGracePeriod.Minimum != 0 || terminationGracePeriod.Default != nil {
					t.Errorf("spec.execution.terminationGracePeriodSeconds schema = %#v", terminationGracePeriod)
				}
				if slices.Contains(execution.Required, "docker") {
					t.Error("spec.execution.docker is required")
				}
				runner := execution.Properties["runner"]
				if !slices.Contains(runner.Required, "image") {
					t.Error("spec.execution.runner.image is not required")
				}
				environment := runner.Properties["env"]
				if environment.MaxItems == nil || *environment.MaxItems != 100 || environment.XListType == nil || *environment.XListType != "map" || !slices.Contains(environment.XListMapKeys, "name") {
					t.Errorf("spec.execution.runner.env schema = %#v", environment)
				}
				resources := runner.Properties["resources"]
				for _, fieldName := range []string{"limits", "requests"} {
					resourceList := resources.Properties[fieldName]
					if resourceList.MaxProperties == nil || *resourceList.MaxProperties != 7 {
						t.Errorf("spec.execution.runner.resources.%s is not bounded to 7 entries", fieldName)
					}
					podResourceList := execution.Properties["resources"].Properties[fieldName]
					if podResourceList.MaxProperties == nil || *podResourceList.MaxProperties != 6 {
						t.Errorf("spec.execution.resources.%s is not bounded to 6 entries", fieldName)
					}
				}
				docker := execution.Properties["docker"]
				if !slices.Contains(docker.Required, "image") {
					t.Error("spec.execution.docker.image is not required")
				}
				dockerEnvironment := docker.Properties["env"]
				if dockerEnvironment.MaxItems == nil || *dockerEnvironment.MaxItems != 100 || dockerEnvironment.XListType == nil || *dockerEnvironment.XListType != "map" || !slices.Contains(dockerEnvironment.XListMapKeys, "name") {
					t.Errorf("spec.execution.docker.env schema = %#v", dockerEnvironment)
				}
			}
			if tt.kind == "RunnerSet" {
				replicas := spec.Properties["replicas"]
				if replicas.Format != "int32" || replicas.Minimum == nil || *replicas.Minimum != 0 || replicas.Default == nil {
					t.Errorf("spec.replicas schema = %#v", replicas)
				}
				template := spec.Properties["template"]
				if !slices.Contains(template.Required, "spec") {
					t.Error("spec.template.spec is not required")
				}
				if version.Subresources.Scale == nil || version.Subresources.Scale.SpecReplicasPath != ".spec.replicas" || version.Subresources.Scale.StatusReplicasPath != ".status.replicas" {
					t.Errorf("scale subresource = %#v", version.Subresources.Scale)
				}
			}
			if tt.kind == "WorkflowJob" {
				outputs := status.Properties["outputs"]
				if outputs.MaxProperties == nil || *outputs.MaxProperties != 100 {
					t.Error("status.outputs is not bounded to 100 entries")
				}
				if len(outputs.XValidations) != 2 {
					t.Errorf("status.outputs validation rules = %d, want 2", len(outputs.XValidations))
				}
			}
			if tt.kind == "WorkflowRun" {
				ttl := spec.Properties["ttlSecondsAfterFinished"]
				if slices.Contains(spec.Required, "ttlSecondsAfterFinished") {
					t.Error("spec.ttlSecondsAfterFinished is required")
				}
				if ttl.Format != "int32" || ttl.Minimum == nil || *ttl.Minimum != 0 || ttl.Maximum == nil || *ttl.Maximum != 2147483647 {
					t.Errorf("spec.ttlSecondsAfterFinished schema = %#v", ttl)
				}
			}
		})
	}
}

func TestCRDRejectsInvalidCELValues(t *testing.T) {
	tests := []struct {
		name   string
		crd    string
		sample string
		mutate func(map[string]any)
	}{
		{
			name: "Project secret reference with empty DNS label",
			crd:  "actions.kelos.dev_projects.yaml", sample: "actions_v1alpha1_project.yaml",
			mutate: func(object map[string]any) {
				github := object["spec"].(map[string]any)["source"].(map[string]any)["github"].(map[string]any)
				github["privateKeySecretRef"].(map[string]any)["name"] = "invalid..name"
			},
		},
		{
			name: "Project source missing selected variant",
			crd:  "actions.kelos.dev_projects.yaml", sample: "actions_v1alpha1_project.yaml",
			mutate: func(object map[string]any) {
				delete(object["spec"].(map[string]any)["source"].(map[string]any), "github")
			},
		},
		{
			name: "Project workflow directory with trailing slash",
			crd:  "actions.kelos.dev_projects.yaml", sample: "actions_v1alpha1_project.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["workflowDirectory"] = ".open-actions/workflows/"
			},
		},
		{
			name: "Project secret reference without a name",
			crd:  "actions.kelos.dev_projects.yaml", sample: "actions_v1alpha1_project-values.yaml",
			mutate: func(object map[string]any) {
				delete(object["spec"].(map[string]any)["secrets"].(map[string]any)["secretRef"].(map[string]any), "name")
			},
		},
		{
			name: "Project secret reference with invalid name",
			crd:  "actions.kelos.dev_projects.yaml", sample: "actions_v1alpha1_project-values.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["secrets"].(map[string]any)["secretRef"].(map[string]any)["name"] = "invalid/name"
			},
		},
		{
			name: "Project variable reference with invalid name",
			crd:  "actions.kelos.dev_projects.yaml", sample: "actions_v1alpha1_project-values.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["variables"].(map[string]any)["configMapRef"].(map[string]any)["name"] = "invalid/name"
			},
		},
		{
			name: "Runner project reference with long DNS label",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["projectRef"].(map[string]any)["name"] = strings.Repeat("a", 64) + ".valid"
			},
		},
		{
			name: "Runner label containing uppercase characters",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["labels"] = []any{"Linux", "linux"}
			},
		},
		{
			name: "Runner label with non-ASCII character",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["labels"] = []any{"Línux"}
			},
		},
		{
			name: "Runner execution with empty image",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["image"] = ""
			},
		},
		{
			name: "Runner execution image containing whitespace",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["image"] = "runner image"
			},
		},
		{
			name: "Runner Docker execution with empty image",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["execution"].(map[string]any)["docker"] = map[string]any{"image": ""}
			},
		},
		{
			name: "Runner resource with invalid name",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
				resources["requests"].(map[string]any)["invalid/name/again"] = "1"
			},
		},
		{
			name: "Runner resource with negative quantity",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
				resources["limits"].(map[string]any)["cpu"] = "-1"
			},
		},
		{
			name: "Runner resource with non-standard unprefixed name",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
				resources["limits"].(map[string]any)["gpu"] = "1"
			},
		},
		{
			name: "Runner resource with fractional extended quantity",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
				resources["limits"].(map[string]any)["example.com/gpu"] = "500m"
			},
		},
		{
			name: "Runner extended resource request without equal limit",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
				resources["requests"].(map[string]any)["example.com/gpu"] = "1"
				resources["limits"].(map[string]any)["example.com/gpu"] = "2"
			},
		},
		{
			name: "Runner resource request greater than limit",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["runner"].(map[string]any)["resources"].(map[string]any)
				resources["requests"].(map[string]any)["cpu"] = "3"
			},
		},
		{
			name: "RunnerSet with negative replicas",
			crd:  "actions.kelos.dev_runnersets.yaml", sample: "actions_v1alpha1_runnerset.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["replicas"] = int64(-1)
			},
		},
		{
			name: "WorkflowJob reference with empty DNS label",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["workflowRunRef"].(map[string]any)["name"] = "invalid..name"
			},
		},
		{
			name: "WorkflowJob ID with invalid syntax",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["jobID"] = "1build"
			},
		},
		{
			name: "WorkflowJob ID exceeding length bound",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["jobID"] = strings.Repeat("a", 257)
			},
		},
		{
			name: "WorkflowJob display name empty",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["displayName"] = ""
			},
		},
		{
			name: "WorkflowJob display name exceeding length bound",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["displayName"] = strings.Repeat("a", 257)
			},
		},
		{
			name: "WorkflowJob label containing uppercase characters",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["runsOn"] = []any{"Linux", "linux"}
			},
		},
		{
			name: "WorkflowJob label with non-ASCII character",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["runsOn"] = []any{"Línux"}
			},
		},
		{
			name: "WorkflowJob timeout below minimum",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["timeoutSeconds"] = int64(59)
			},
		},
		{
			name: "WorkflowJob timeout above maximum",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["timeoutSeconds"] = int64(9223372037)
			},
		},
		{
			name: "WorkflowJob output with invalid name",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["status"] = map[string]any{"outputs": map[string]any{"invalid.name": "value"}}
			},
		},
		{
			name: "WorkflowJob output exceeding value bound",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["status"] = map[string]any{"outputs": map[string]any{"value": strings.Repeat("x", 4097)}}
			},
		},
		{
			name: "WorkflowJob dependency with invalid syntax",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["needs"] = []any{"build.test"}
			},
		},
		{
			name: "WorkflowJob invalid result",
			crd:  "actions.kelos.dev_workflowjobs.yaml", sample: "actions_v1alpha1_workflowjob.yaml",
			mutate: func(object map[string]any) {
				object["status"] = map[string]any{"result": "neutral"}
			},
		},
		{
			name: "WorkflowRun reference with empty DNS label",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["projectRef"].(map[string]any)["name"] = "invalid..name"
			},
		},
		{
			name: "WorkflowRun source missing selected variant",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				delete(object["spec"].(map[string]any)["source"].(map[string]any), "github")
			},
		},
		{
			name: "WorkflowRun path with uppercase extension",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["workflowPath"] = ".open-actions/workflows/ci.YAML"
			},
		},
		{
			name: "WorkflowRun negative TTL",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["ttlSecondsAfterFinished"] = int64(-1)
			},
		},
		{
			name: "WorkflowRun intermediate SHA length",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["sha"] = strings.Repeat("a", 41)
			},
		},
		{
			name: "WorkflowRun invalid head SHA length",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["headSHA"] = strings.Repeat("a", 41)
			},
		},
		{
			name: "WorkflowRun non-pull request with head SHA",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				event := github["event"].(map[string]any)
				event["name"] = "push"
				delete(event, "action")
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "headRef")
			},
		},
		{
			name: "WorkflowRun ref with empty component",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["ref"] = "refs//main"
			},
		},
		{
			name: "WorkflowRun ref without a name",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["ref"] = "refs/"
			},
		},
		{
			name: "WorkflowRun ref with traversal component",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["ref"] = "refs/heads/../main"
			},
		},
		{
			name: "WorkflowRun ref with forbidden character",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["ref"] = "refs/heads/main~1"
			},
		},
		{
			name: "WorkflowRun ref with control character",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["ref"] = "refs/heads/main\x01"
			},
		},
		{
			name: "WorkflowRun pull request without head branch",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				delete(workflowRunGitHub(object)["revision"].(map[string]any), "headRef")
			},
		},
		{
			name: "WorkflowRun non-pull request with head branch",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				event := github["event"].(map[string]any)
				event["name"] = "push"
				delete(event, "action")
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "baseRef")
			},
		},
		{
			name: "WorkflowRun push with base branch",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				event := github["event"].(map[string]any)
				event["name"] = "push"
				delete(event, "action")
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "headRef")
			},
		},
		{
			name: "WorkflowRun malformed head branch",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["headRef"] = "feature..branch"
			},
		},
		{
			name: "WorkflowRun head branch with leading dot",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["headRef"] = ".feature"
			},
		},
		{
			name: "WorkflowRun head branch with refs prefix",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["headRef"] = "refs/heads/main"
			},
		},
		{
			name: "WorkflowRun head branch resembling object ID",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["headRef"] = strings.Repeat("a", 40)
			},
		},
		{
			name: "WorkflowRun malformed base branch",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["baseRef"] = "main..backup"
			},
		},
		{
			name: "WorkflowRun unsupported event name",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["event"].(map[string]any)["name"] = "deployment"
			},
		},
		{
			name: "WorkflowRun schedule without cron expression",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				setBranchEvent(workflowRunGitHub(object), "schedule", "")
			},
		},
		{
			name: "WorkflowRun schedule with malformed cron expression",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				setBranchEvent(github, "schedule", "")
				github["event"].(map[string]any)["schedule"] = "0 6 * *"
			},
		},
		{
			name: "WorkflowRun push with inputs",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				setBranchEvent(github, "push", "")
				github["event"].(map[string]any)["inputs"] = map[string]any{"value": "unexpected"}
			},
		},
		{
			name: "WorkflowRun webhook event without delivery ID",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				delete(workflowRunGitHub(object)["event"].(map[string]any), "deliveryID")
			},
		},
		{
			name: "WorkflowRun manual event with delivery ID",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun-dispatch.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["event"].(map[string]any)["deliveryID"] = "manual-delivery"
			},
		},
		{
			name: "WorkflowRun push with action",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				event := github["event"].(map[string]any)
				event["name"] = "push"
				event["action"] = "closed"
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "headRef")
			},
		},
		{
			name: "WorkflowRun unsupported pull request action",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["event"].(map[string]any)["action"] = "created"
			},
		},
		{
			name: "WorkflowRun pull request with branch ref",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["ref"] = "refs/heads/main"
			},
		},
		{
			name: "WorkflowRun merge group with branch ref",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				event := github["event"].(map[string]any)
				event["name"] = "merge_group"
				event["action"] = "checks_requested"
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/main"
				delete(revision, "headRef")
			},
		},
		{
			name: "WorkflowRun unsupported merge group action",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				github := workflowRunGitHub(object)
				event := github["event"].(map[string]any)
				event["name"] = "merge_group"
				event["action"] = "queued"
				revision := github["revision"].(map[string]any)
				revision["ref"] = "refs/heads/gh-readonly-queue/main/pr-42"
				delete(revision, "headRef")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crd, _ := loadCRD(t, tt.crd)
			object := loadSample(t, tt.sample)
			tt.mutate(object)
			if errs := validateObject(t, crd, object, nil); len(errs) == 0 {
				t.Fatal("invalid object passed OpenAPI and CEL validation")
			}
		})
	}
}

func workflowRunGitHub(object map[string]any) map[string]any {
	return object["spec"].(map[string]any)["source"].(map[string]any)["github"].(map[string]any)
}

func TestWorkflowJobRunnerAssignmentIsSetOnce(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowjobs.yaml")
	queued := loadWorkflowJobSample(t)
	assigned := loadWorkflowJobSample(t)
	assigned["status"] = map[string]any{"runnerRef": map[string]any{"name": "runner-1"}}
	if errs := validateObject(t, crd, assigned, queued); len(errs) > 0 {
		t.Fatalf("initial runner assignment was rejected: %v", errs.ToAggregate())
	}

	reassigned := loadWorkflowJobSample(t)
	reassigned["status"] = map[string]any{"runnerRef": map[string]any{"name": "runner-2"}}
	if errs := validateObject(t, crd, reassigned, assigned); len(errs) == 0 {
		t.Fatal("runner reassignment passed CEL validation")
	}
}

func TestWorkflowJobResultIsSetOnce(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowjobs.yaml")
	queued := loadWorkflowJobSample(t)
	completed := loadWorkflowJobSample(t)
	completed["status"] = map[string]any{"result": "success"}
	if errs := validateObject(t, crd, completed, queued); len(errs) > 0 {
		t.Fatalf("initial result was rejected: %v", errs.ToAggregate())
	}

	changed := loadWorkflowJobSample(t)
	changed["status"] = map[string]any{"result": "failure"}
	if errs := validateObject(t, crd, changed, completed); len(errs) == 0 {
		t.Fatal("result update passed CEL validation")
	}
}

func loadWorkflowJobSample(t *testing.T) map[string]any {
	t.Helper()
	object := loadSample(t, "actions_v1alpha1_workflowjob.yaml")
	spec := object["spec"].(map[string]any)
	if timeout, ok := spec["timeoutSeconds"].(float64); ok {
		spec["timeoutSeconds"] = int64(timeout)
	}
	return object
}

func validateSample(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition, name string) {
	t.Helper()
	object := loadSample(t, name)
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("sample does not satisfy CRD schema: %v", errs.ToAggregate())
	}
}

func loadSample(t *testing.T, name string) map[string]any {
	t.Helper()
	path := filepath.Join("..", "..", "config", "samples", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	object := map[string]any{}
	if err := yaml.Unmarshal(data, &object); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return object
}

func validateObject(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition, object, oldObject map[string]any) field.ErrorList {
	t.Helper()
	internal := convertCRD(t, crd)
	schemaProps, err := apiextensions.GetSchemaForVersion(internal, "v1alpha1")
	if err != nil {
		t.Fatalf("get v1alpha1 schema: %v", err)
	}
	validator, _, err := customresourcevalidation.NewSchemaValidator(schemaProps.OpenAPIV3Schema)
	if err != nil {
		t.Fatalf("create schema validator: %v", err)
	}
	errs := customresourcevalidation.ValidateCustomResource(field.NewPath("resource"), object, validator)
	structural, err := structuralschema.NewStructural(schemaProps.OpenAPIV3Schema)
	if err != nil {
		t.Fatalf("create structural schema: %v", err)
	}
	errs = append(errs, structurallisttype.ValidateListSetsAndMaps(field.NewPath("resource"), structural, object)...)
	celValidator := celvalidation.NewValidator(structural, true, celconfig.PerCallLimit)
	if celValidator != nil {
		celErrors, _ := celValidator.Validate(context.Background(), field.NewPath("resource"), structural, object, oldObject, celconfig.RuntimeCELCostBudget)
		errs = append(errs, celErrors...)
	}
	return errs
}

func validateCRD(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) {
	t.Helper()
	internal := convertCRD(t, crd)
	internal.Status.StoredVersions = []string{"v1alpha1"}
	if errs := apiextensionsvalidation.ValidateCustomResourceDefinition(context.Background(), internal); len(errs) > 0 {
		t.Fatalf("invalid CRD: %v", errs.ToAggregate())
	}
}

func convertCRD(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) *apiextensions.CustomResourceDefinition {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := apiextensions.AddToScheme(scheme); err != nil {
		t.Fatalf("add internal apiextensions types to scheme: %v", err)
	}
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1 apiextensions types to scheme: %v", err)
	}

	internal := &apiextensions.CustomResourceDefinition{}
	if err := scheme.Convert(crd, internal, nil); err != nil {
		t.Fatalf("convert CRD to internal type: %v", err)
	}
	return internal
}

func loadCRD(t *testing.T, name string) (*apiextensionsv1.CustomResourceDefinition, int) {
	t.Helper()
	path := filepath.Join("..", "manifests", "charts", "open-actions", "templates", "crds", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(data, crd); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return crd, len(data)
}

func storageVersion(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) apiextensionsv1.CustomResourceDefinitionVersion {
	t.Helper()
	for _, version := range crd.Spec.Versions {
		if version.Storage {
			return version
		}
	}
	t.Fatal("CRD has no storage version")
	return apiextensionsv1.CustomResourceDefinitionVersion{}
}
