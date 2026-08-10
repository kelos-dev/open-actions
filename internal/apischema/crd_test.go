package apischema

import (
	"context"
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

func TestWorkflowRunAcceptsGitHubEventContracts(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	for _, tt := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "closed merged pull request",
			mutate: func(github map[string]any) {
				github["event"].(map[string]any)["action"] = "closed"
				github["revision"].(map[string]any)["ref"] = "refs/heads/main"
			},
		},
		{
			name: "closed unmerged pull request",
			mutate: func(github map[string]any) {
				github["event"].(map[string]any)["action"] = "closed"
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
				delete(revision, "headRef")
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

func TestWorkflowRunAcceptsAtBranchName(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_workflowruns.yaml")
	object := loadSample(t, "actions_v1alpha1_workflowrun.yaml")
	workflowRunGitHub(object)["revision"].(map[string]any)["headRef"] = "@"
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("valid @ branch name was rejected: %v", errs.ToAggregate())
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

func TestRunnerAcceptsQualifiedResourceNames(t *testing.T) {
	crd, _ := loadCRD(t, "actions.kelos.dev_runners.yaml")
	object := loadSample(t, "actions_v1alpha1_runner.yaml")
	resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
	resources["requests"].(map[string]any)["example.com/accelerator"] = "1"
	resources["limits"].(map[string]any)["example.com/accelerator"] = "1"
	if errs := validateObject(t, crd, object, nil); len(errs) > 0 {
		t.Fatalf("qualified resource name was rejected: %v", errs.ToAggregate())
	}
}

func TestCRDConventions(t *testing.T) {
	tests := []struct {
		file         string
		sample       string
		kind         string
		requiredSpec []string
		immutable    bool
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
			file:         "actions.kelos.dev_workflowjobs.yaml",
			sample:       "actions_v1alpha1_workflowjob.yaml",
			kind:         "WorkflowJob",
			requiredSpec: []string{"jobID", "runsOn", "workflowRunRef"},
			immutable:    true,
		},
		{
			file:         "actions.kelos.dev_workflowruns.yaml",
			sample:       "actions_v1alpha1_workflowrun.yaml",
			kind:         "WorkflowRun",
			requiredSpec: []string{"projectRef", "source", "workflowPath"},
			immutable:    true,
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

			if tt.immutable && len(spec.XValidations) == 0 {
				t.Error("immutable spec has no CEL validation")
			}

			if tt.kind == "Runner" {
				execution := spec.Properties["execution"]
				if !slices.Contains(execution.Required, "image") {
					t.Error("spec.execution.image is not required")
				}
				resources := execution.Properties["resources"]
				for _, fieldName := range []string{"limits", "requests"} {
					resourceList := resources.Properties[fieldName]
					if resourceList.MaxProperties == nil || *resourceList.MaxProperties != 7 {
						t.Errorf("spec.execution.resources.%s is not bounded to 7 entries", fieldName)
					}
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
				object["spec"].(map[string]any)["execution"].(map[string]any)["image"] = ""
			},
		},
		{
			name: "Runner execution image containing whitespace",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				object["spec"].(map[string]any)["execution"].(map[string]any)["image"] = "runner image"
			},
		},
		{
			name: "Runner resource with invalid name",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
				resources["requests"].(map[string]any)["invalid/name/again"] = "1"
			},
		},
		{
			name: "Runner resource with negative quantity",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
				resources["limits"].(map[string]any)["cpu"] = "-1"
			},
		},
		{
			name: "Runner resource with non-standard unprefixed name",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
				resources["limits"].(map[string]any)["gpu"] = "1"
			},
		},
		{
			name: "Runner resource with fractional extended quantity",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
				resources["limits"].(map[string]any)["example.com/gpu"] = "500m"
			},
		},
		{
			name: "Runner extended resource request without equal limit",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
				resources["requests"].(map[string]any)["example.com/gpu"] = "1"
				resources["limits"].(map[string]any)["example.com/gpu"] = "2"
			},
		},
		{
			name: "Runner resource request greater than limit",
			crd:  "actions.kelos.dev_runners.yaml", sample: "actions_v1alpha1_runner.yaml",
			mutate: func(object map[string]any) {
				resources := object["spec"].(map[string]any)["execution"].(map[string]any)["resources"].(map[string]any)
				resources["requests"].(map[string]any)["cpu"] = "3"
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
			name: "WorkflowRun intermediate SHA length",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["revision"].(map[string]any)["sha"] = strings.Repeat("a", 41)
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
				github["revision"].(map[string]any)["ref"] = "refs/heads/main"
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
			name: "WorkflowRun unsupported event name",
			crd:  "actions.kelos.dev_workflowruns.yaml", sample: "actions_v1alpha1_workflowrun.yaml",
			mutate: func(object map[string]any) {
				workflowRunGitHub(object)["event"].(map[string]any)["name"] = "schedule"
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
	queued := loadSample(t, "actions_v1alpha1_workflowjob.yaml")
	assigned := loadSample(t, "actions_v1alpha1_workflowjob.yaml")
	assigned["status"] = map[string]any{"runnerRef": map[string]any{"name": "runner-1"}}
	if errs := validateObject(t, crd, assigned, queued); len(errs) > 0 {
		t.Fatalf("initial runner assignment was rejected: %v", errs.ToAggregate())
	}

	reassigned := loadSample(t, "actions_v1alpha1_workflowjob.yaml")
	reassigned["status"] = map[string]any{"runnerRef": map[string]any{"name": "runner-2"}}
	if errs := validateObject(t, crd, reassigned, assigned); len(errs) == 0 {
		t.Fatal("runner reassignment passed CEL validation")
	}
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
