package main

import (
	"encoding/json"
	githubclient "github.com/kelos-dev/open-actions/internal/github"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kelos-dev/open-actions/internal/workflow"
)

func TestTokenPermissionsWorkflow(t *testing.T) {
	definition, err := workflow.Parse([]byte(tokenPermissionsWorkflowData))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]workflow.Permissions{
		"inherited":  {"issues": "write"},
		"overridden": {"statuses": "read"},
		"disabled":   {},
	}
	if len(definition.Jobs) != len(want) {
		t.Fatalf("jobs = %#v", definition.Jobs)
	}
	for jobID, permissions := range want {
		job, found := definition.Jobs[jobID]
		if !found {
			t.Fatalf("job %q is missing", jobID)
		}
		effective := workflow.EffectivePermissions(definition.Permissions, job.Permissions)
		if !maps.Equal(effective, permissions) {
			t.Fatalf("job %q permissions = %#v, want %#v", jobID, effective, permissions)
		}
	}
}

func TestPreparationWorkflowIsolatesDownloadBlock(t *testing.T) {
	definition, err := workflow.Parse([]byte(preparationWorkflowData))
	if err != nil {
		t.Fatal(err)
	}
	steps := definition.Jobs["test"].Steps
	blockIndex := -1
	actionIndex := -1
	for index, step := range steps {
		if strings.Contains(step.Run, "/fixture/block-preparation-actions") {
			blockIndex = index
		}
		if step.Uses == "preparation-actions/composite@v1" {
			actionIndex = index
		}
	}
	if blockIndex < 0 || actionIndex < 0 || blockIndex >= actionIndex {
		t.Fatalf("preparation steps = %#v", steps)
	}

	standard, err := workflow.Parse([]byte(workflowData))
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range standard.Jobs["test"].Steps {
		if strings.Contains(step.Run, "/fixture/block-preparation-actions") || strings.HasPrefix(step.Uses, "preparation-actions/") {
			t.Fatalf("standard workflow contains preparation probe: %#v", step)
		}
	}
}

func TestConcurrencyWorkflowsAreValid(t *testing.T) {
	for name, data := range map[string]string{
		"job concurrency":      jobConcurrencyWorkflowData,
		"concurrency conflict": concurrencyConflictWorkflowData,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := workflow.Parse([]byte(data)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSelectiveRerunWorkflowIsValid(t *testing.T) {
	for _, data := range []string{selectiveRerunWorkflowData, selectiveMatrixRerunWorkflowData} {
		definition, err := workflow.Parse([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		if len(definition.Jobs) != 2 || len(definition.Jobs["verify"].Needs) != 1 || definition.Jobs["verify"].Needs[0] != "prepare" {
			t.Fatalf("jobs = %#v", definition.Jobs)
		}
	}
}

func TestCacheFixtureWorkflow(t *testing.T) {
	definition, err := workflow.Parse([]byte(cacheWorkflowData))
	if err != nil {
		t.Fatal(err)
	}
	if definition.Jobs["cached"].Steps[0].Uses != "actions/cache@v4" || definition.Jobs["cached"].Outputs["hit"] != "${{ steps.cache.outputs.cache-hit }}" {
		t.Fatal(definition)
	}
}

func TestRepositoryMetadataRoutes(t *testing.T) {
	mux := http.NewServeMux()
	registerRepositoryMetadata(mux)
	for _, name := range []string{"example", "invalid-trigger", "invalid-field"} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/repos/acme/"+name, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("repository metadata status = %d", response.Code)
			}
			var repository githubclient.Repository
			if err := json.Unmarshal(response.Body.Bytes(), &repository); err != nil {
				t.Fatal(err)
			}
			if repository.ID != 123456789 || repository.Name != name || repository.DefaultBranch != "main" || repository.Owner.Login != "acme" {
				t.Fatalf("repository = %#v", repository)
			}
		})
	}
}
